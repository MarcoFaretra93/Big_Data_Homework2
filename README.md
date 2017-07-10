# Big_Data_Homework_2
# Marco Faretra	Gabriele Marini

## Requisiti

I requisiti per l'avvio del progetto in locale sono:
* Un'installazione di hadoop (noi abbiamo usato la 2.8)
* L'installazione di spark 2.1.1 (non versioni inferiori)
* L'installazione di mongoDB
* L'installazione di Redis

Per redis e mongoDB, noi abbiamo utilizzato docker, attraverso i seguenti comandi:

* Per mongoDB:

```
docker run -p 27017:27017 --name mongo -d mongo
```

Comando che crea un container di nome mongo, con l'installazione al suo interno.

* Per Redis:

```
docker run -p 6379:6379 --name redis -d redis
```

Comando che crea un container di nome redis, con l'installazione al suo interno.

## Esecuzione 

Per il popolamento dei db (Redis e Mongo), basta avviare il seguente script con i seguenti parametri: 

```
./init.sh populate
```

Dopo il popolamento, se si vogliono avviare i job spark (tutte le categorie definite): 

* Raggruppando lo score della categoria per college:

```
./init.sh run_spark -c
```

* Raggruppando lo score della categoria per giocatore:

```
./init.sh run_spark
```

Altrimenti se si vuole avviare tutto insieme (popolamento più esecuzione di tutti i job):

* Per college:

```
./init.sh all -c
```

* Per giocatore:

```
./init.sh all
```

