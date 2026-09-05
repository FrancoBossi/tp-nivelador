Redactar un breve informe en donde se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado y los mecanismos para sincronizar la ejecución concurrente.

# Informe de la solución

## Protocolo de comunicación

La comunicación entre el cliente Go y el servidor Python se realiza mediante
sockets TCP y un protocolo de tramas con longitud prefijada. Cada trama está
compuesta por un encabezado de 4 bytes que indica, en formato big-endian, la
longitud del payload, seguido por el payload correspondiente.

Las apuestas se serializan manualmente en formato CSV. El cliente envía los datos de las apuestas
en lotes configurables mediante `BATCH_SIZE` y finaliza el envío con la trama
`__END__`. El servidor deserializa los datos, los transforma en objetos del
dominio y los almacena mediante `Lottery.store_bets()`. Al finalizar la
recepción, obtiene las apuestas persistidas con `Lottery.load_bets()`, verifica
los ganadores con `Lottery.has_won()` y devuelve a cada agencia únicamente sus
propios ganadores.

La lógica de framing y serialización está separada de la lógica de negocio en
los módulos `cliente-protocolo` y `server-protocolo`. Las funciones
`safe_socket` garantizan la recepción y el envío completo de los bytes
solicitados, contemplando short read, short write, cierres prematuros y
errores de comunicación.

## Concurrencia y sincronización

El servidor utiliza un thread aceptador y un thread independiente por cliente,
lo que permite procesar conexiones concurrentemente.

La sincronización se implementa mediante mecanismos explícitos:

- `lottery_lock` protege el acceso concurrente al almacenamiento de apuestas.
- `client_threads_lock` protege las colecciones de sockets y threads activos.
- `round_lock`, implementado con `threading.Condition`, coordina la espera del
  quórum y la distribución de resultados.
- `shutdown_lock` garantiza que el cierre sea idempotente.

El quórum mínimo de agencias se configura mediante `AGENCY_QUORUM_MIN`. Cuando
se alcanza, el servidor procesa atómicamente la ronda y asocia los resultados
con cada `agency_id`, evitando enviar ganadores de una agencia a otra.

El cierre ante `SIGTERM` se realiza de forma graceful: se notifica a los
threads bloqueados, se cierran los sockets activos y se espera la finalización
de los threads y archivos abiertos. El cliente utiliza un contexto cancelable
para interrumpir operaciones de red bloqueadas y liberar sus recursos.
