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