Redactar un breve informe en donde se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado y los mecanismos para sincronizar la ejecución concurrente.

## Informe del ejercicio 5

## Protocolo de comunicación

Se implementó un **protocolo de tramas con longitud prefijada**. Cada mensaje se estructura en:
- **Header (4 bytes):** Longitud del payload en formato big-endian (uint32)
- **Payload:** Datos en formato CSV

El cliente serializa cada apuesta como `agency_id,first_name,last_name,document,birthdate,number` y la envía en una trama. Luego de todas las apuestas, envía una trama de cierre con payload `__END__`.

El servidor deserializa cada trama, la convierte en un objeto `Bet` y la almacena mediante `Lottery.store_bets()`. Cuando recibe `__END__`, calcula los ganadores filtrando con `Lottery.load_bets()` y `Lottery.has_won()`, y devuelve al cliente solo los ganadores de esa agencia (sin agency_id) en una trama de respuesta.

La capa `safe_socket` maneja los casos de short read/write completando las lecturas y escrituras hasta obtener la cantidad exacta de bytes indicada en el header.

## Sincronización y concurrencia

El servidor acepta conexiones concurrentes en threads independientes. La sincronización se basa en **intercambio de mensajes** sin tiempos prefijados:
- El cliente envía tramas y espera respuestas
- El servidor procesa tramas conforme llegan
- El mensaje `__END__` actúa como señal de fin de transmisión

Para evitar carreras de datos sobre el archivo de almacenamiento, el acceso a `Lottery.store_bets()` y `Lottery.load_bets()` está protegido por un lock (mutex) compartido. Esto garantiza consistencia sin deadlocks.


## Informe del ejercicio 6

## Procesamiento por lotes (batching)

Para reducir la cantidad de tramas enviadas y acelerar la transmisión, se modificó el cliente para agrupar varias apuestas dentro de un mismo mensaje. La cantidad de registros por lote queda configurada por la variable de entorno `BATCH_SIZE` y se valida como un entero positivo. En caso de no especificarse, el comportamiento por defecto equivale a un lote de 1 apuesta, manteniendo compatibilidad con el protocolo anterior.

La serialización del cliente ya no envía una apuesta por trama: en cambio, acumula un conjunto de filas del archivo de entrada y las empaqueta como un único payload, separando cada apuesta con un salto de línea. 

Luego del último lote, el cliente envía el mensaje `__END__` para indicar que la transmisión terminó. El servidor, por su parte, recibe cada trama y la deserializa como un batch. Cada línea del payload se interpreta como una apuesta individual; si el lote llega bien formado, se procesa completo y se agrega al almacenamiento. Si alguna línea es inválida, la conexión se corta con error y no se deja un estado parcial de ese lote.

Esta estrategia conserva la consistencia del protocolo y asegura que la respuesta del servidor se devuelve solo cuando el lote completo es válido. La carga de apuestas se mantiene a nivel de dominio, mientras que la capa de comunicación se encarga exclusivamente del empaquetado y desempaquetado del batch.


## Informe del ejercicio 7

## Concurrencia y Sincronización de Rondas de Sorteo

### Concepto General

El ejercicio 7 introduce el problema de **múltiples agencias conectándose simultáneamente** al servidor. Para garantizar que se realiza un sorteo justo e íntegro, el servidor debe:

1. **Esperar un quórum mínimo** de agencias antes de ejecutar un sorteo
2. **Ejecutar el sorteo de forma atómica** cuando se alcanza el quórum
3. **Distribuir resultados scoped**: cada agencia recibe **SOLO los ganadores de sus propias apuestas**, no de todas
4. **Permitir rondas múltiples** sin reiniciar el servidor

### Implementación: Sistema de Rondas

El servidor implementa un **modelo de rondas de sorteo** usando sincronización con `threading.Condition`:

#### Estructura de datos:
```python
self.round_bets = {}           # {agency_id: [Bets]}
self.round_results = {}        # {agency_id: [winners]}
self.agency_quorum_min = int   # Variable de entorno
self.round_lock = threading.Condition()
```

#### Flujo de una ronda:

1. **Fase de acumulación**: Cuando una agencia se conecta:
   - Se añaden sus apuestas a `round_bets[agency_id]`
   - Si `len(round_bets) < agency_quorum_min`: el thread espera con `round_lock.wait()`

2. **Fase de disparo**: Cuando se alcanza el quórum:
   - Se captura un snapshot de `round_bets`
   - Se limpian los datos para la próxima ronda: `round_bets.clear()`
   - Se ejecuta `_compute_round_winners(snapshot)` que:
     - Almacena todas las apuestas en la BD con `Lottery.store_bets()`
     - Calcula ganadores filtrando por agency_id
     - Cada agencia recibe SOLO sus ganadores (constraint clave)

3. **Fase de distribución**: Los resultados se distribuyen:
   - Se colocan en `round_results[agency_id]`
   - Se notifica a todos los threads esperando: `round_lock.notify_all()`
   - Cada thread recibe su respuesta específica

4. **Fase de limpieza**: Los resultados se consumen:
   - `response = round_results.pop(agency_id)`
   - El thread retorna la respuesta al cliente

#### Garantías de seguridad:

- **Atomicidad del sorteo**: El bloque `with self.round_lock` garantiza que la transición de acumulación a cómputo es indivisible
- **No hay broadcast**: Solo los ganadores que pertenecen al `agency_id` de la agencia se devuelven
- **Rondas independientes**: Cada ronda es un ciclo completo sin interferencias
- **No hay deadlock**: El condition variable se usa correctamente: espera dentro del lock y notificación antes de liberar

### Concurrencia a nivel de threads

El servidor implementa **un thread por cliente**:
```python
threading.Thread(
    target=self._handle_client,
    args=(client_socket,),
    daemon=True,
).start()
```

Todos los threads comparten:
- `lottery_lock` (mutex): protege accesos a `Lottery.store_bets()` y `Lottery.load_bets()`
- `round_lock` (condition variable): coordina el sincronismo entre agencias

### Comunicación

El protocolo de comunicación se mantiene igual:
- **Cliente → Servidor**: Tramas con batches de apuestas + `__END__`
- **Servidor → Cliente**: Una única trama con ganadores (solo los de esa agencia)

La novedad es que el servidor **no responde inmediatamente**: espera a que lleguen suficientes agencias antes de computar ganadores.

## Informe del ejercicio 8

El cierre graceful comienza al recibir `SIGTERM` (o `SIGINT` para facilitar la ejecución local) y no intercambia mensajes adicionales con el otro proceso.

### Cliente

`signal.NotifyContext` cancela el contexto de ejecución. La cancelación cierra el socket para interrumpir inmediatamente cualquier lectura o escritura bloqueada. Al finalizar `Run`, se cierran el archivo de entrada, el archivo de salida y la goroutine encargada de cerrar la conexión.

### Servidor

El handler de señales:

- marca el evento de apagado;
- despierta las agencias bloqueadas en la condición de quórum;
- cierra el socket de escucha y todos los sockets de clientes activos.

El hilo principal espera al hilo aceptador y a todos los threads de clientes antes de terminar. Los sockets se cierran también mediante `finally` y context managers, por lo que no quedan descriptores abiertos. Las apuestas parciales de una ronda se descartan al apagar el servidor, evitando ejecutar un sorteo durante el cierre.

El proceso no espera sleeps ni realiza un protocolo de despedida: libera los recursos tan pronto como recibe `SIGTERM`, dentro del tiempo acotado por Docker.