# TP Nivelador — Sistemas Distribuidos I (75.74)

**Alumno:** Franco Martín Almada
**Padrón:** 104498

---

## Protocolo

Se diseñó un protocolo binario donde cada mensaje enviado entre el cliente y
el servidor tiene la siguiente estructura en bytes:

```
[4 bytes: longitud del cuerpo, big-endian] [1 byte: tipo de mensaje] [payload]
```

Los primeros cuatro bytes indican, de forma fija, cuántos bytes miden el
resto del mensaje (tipo más payload). El receptor primero lee esos 4 bytes,
los interpreta como un entero sin signo de 32 bits, y luego lee exactamente
la cantidad de bytes que indica. Esto evita cualquier ambigüedad con respecto
a dónde empieza y termina el mensaje, sin que cada uno tenga que tener un
tamaño fijo.

### Tipos de mensaje

| Tipo | Byte | Quién lo envía | Propósito |
|---|---|---|---|
| `HELLO`     | `H` | Cliente  | Identifica el `agency_id` de la conexión. |
| `BET_BATCH` | `B` | Cliente  | Transporta una o más apuestas (campos separados por coma). |
| `DONE`      | `D` | Cliente  | Indica que la agencia no tiene más apuestas para enviar. |
| `ACK`       | `A` | Servidor | Confirma cuántas apuestas fueron almacenadas. |
| `WINNERS`   | `W` | Servidor | Devuelve los ganadores de esa agencia únicamente. |

## Batching

Para reducir la cantidad de mensajes en la red, el cliente agrupa hasta
`BATCH_SIZE` apuestas, configurable por variable de entorno, en un único
mensaje, en lugar de enviar cada una en uno diferente.

## Short writes and reads

Como una llamada a `send` o `receive` no garantiza mover todos los bytes
solicitados de una vez, se implementaron *wrappers* que ciclan hasta
completar exactamente la cantidad de bytes pedida.

- `send_all(sock, data)` (Python) / `SendAll(writer, data)` (Go):
  reintenta hasta que la totalidad de `data` fue efectivamente escrita.
- `recv_all(sock, size)` (Python) / `RecvAll(reader, size)` (Go):
  reintenta hasta acumular exactamente `size` bytes, o lanza un error
  si la conexión se cierra antes de completar la lectura.

## Sincronización de la ejecución concurrente

### Servidor multi-thread

El servidor atiende múltiples agencias en simultáneo lanzando un hilo por
cada conexión aceptada, en lugar de procesarlas de forma serial. El hilo
principal solo se encarga de aceptar conexiones nuevas y delegar su atención
a un hilo dedicado, sin esperar que finalicen las conexiones anteriores.

### Quorum de agencias

El sorteo no se calcula apenas una agencia individual termina de enviar sus
apuestas: el servidor espera a que un mínimo configurable,
`AGENCY_QUORUM_MIN`, haya finalizado, antes de habilitar el cálculo de
ganadores para cualquiera de ellas.

Esto se implementó con una clase, `DrawCoordinator`, que utiliza un
`threading.Condition` para coordinar los hilos:

- Cada hilo, al terminar de recibir las apuestas de su agencia, llama a
  `notify_finished_and_wait()`. Esto incrementa, bajo lock, un contador
  compartido de agencias finalizadas.
- Si con ese incremento se alcanza el quorum, se marca una bandera
  (`_released`) como verdadera y se despiertan **todos** los hilos que
  estuvieran esperando (`notify_all()`).
- Si el quorum aún no se alcanzó, el hilo se bloquea (`wait()`) sin consumir
  CPU, hasta ser notificado.
- Una vez que la bandera `_released` queda en verdadero, cualquier agencia
  que termine **después** no necesita esperar: la condición ya está abierta
  para el resto de la ejecución.

Este mecanismo evita la clásica condición de carrera de un contador
compartido (dos hilos incrementándolo "al mismo tiempo" y perdiendo una de
las sumas), ya que toda lectura/escritura del contador ocurre dentro de la
sección protegida por el lock implícito del `Condition`.

### Acceso al almacenamiento compartido

Las apuestas se persisten en un único archivo
(`Lottery.store_bets`/`load_bets`), al que acceden concurrentemente todos los
hilos activos. Para evitar escrituras/lecturas entrelazadas que corrompieran
el archivo, todo acceso a `Lottery` se protege con un `threading.Lock`
(`_storage_lock`) exclusivo, tanto al almacenar un batch de apuestas como al
recuperarlas para calcular ganadores.

### Manejo de `SIGTERM`

Tanto el cliente como el servidor liberan sus recursos de forma prolija al
recibir la señal `SIGTERM`, dentro del tiempo de gracia otorgado por Docker:

- **Servidor (Python):** se registra un manejador de señal
  (`signal.signal(SIGTERM, ...)`) que invoca `Server.shutdown()`. Este
  método cierra el socket que acepta conexiones (lo que desbloquea un
  `accept()` en curso) y cierra explícitamente cada socket de cliente activo
  (lo que desbloquea cualquier `recv_message` bloqueado), además de forzar
  la liberación de hilos que estuvieran esperando el quorum mediante
  `_DrawCoordinator.release_all()`.
- **Cliente (Go):** una goroutine separada escucha la señal del sistema
  operativo mediante un canal (`signal.Notify`), y al recibirla cierra la
  conexión activa (`conn.Close()`), lo que desbloquea cualquier
  `Read`/`Write` en curso. Una bandera atómica (`atomic.Bool`) permite
  distinguir un error de red genuino de uno provocado intencionalmente por
  el propio shutdown, de forma que el proceso termine con código de salida
  `0` en este último caso.
