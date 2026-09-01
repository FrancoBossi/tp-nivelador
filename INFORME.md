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