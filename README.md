# Big_Data_Homework_2
# Marco Faretra	Gabriele Marini

## Requisiti

I requisiti per l'avvio del progetto in locale sono:
* Python 2.7
* Un'installazione di hadoop (noi abbiamo usato la 2.8)
* L'installazione di spark 2.1.1 (non versioni inferiori)
* L'installazione di mongoDB
* L'installazione di Redis

Per redis e mongoDB, noi si è utilizzato docker, attraverso i seguenti comandi:

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

È possibile vedere la documentazione completa dei parametri attraverso il comando: 

```
python init.py --help
```

### Popolamento dei DB

Per popolare MongoDB e Redis, basta avviare lo script python, init.py con il parametro populate e specificando l'ip sul quale sono installati i databases. Ad esempio: 

```
python init.py all -c -dp mongo -ip localhost
```

Successivamente è possibile avviare l'esecuzioni di spark sulle seguenti categorie:

* attackers
* defenders
* 2_point_shooters
* 3_point_shooters
* rebounders
* plus_minus
* all_around

Specificandone la categoria dei giocatori è possibile quindi invocare le esecuzioni, di seguito alcuni esempi:

Ad esempio per prendere tutti gli attaccanti ordinati per score del giocatore (utilizzando mongo come base di dati predefinita):

```
python init.py attackers -dp mongo -ip localhost
```

Prendere i college ordinati per score, in base a quello dei giocatori di appartenza:

```
python init.py defenders -dp mongo -ip localhost -c 
```

Se voglio avviare la mia esecuzione in ambiente distribuito:

```
python init.py defenders -dp mongo -ip localhost -c --dist
```

O posso anche utilizzare redis come base di dati predefinita, ad esempio: 

```
python init.py attackers -dp redis -ip localhost
```

Se invece volessi lanciare insieme tutte le categorie, basta inserire come categoria "all", ad esempio: 

* Ordinati per score del giocatore:

```
python init.py all -dp mongo -ip localhost 
``` 

* Ordinati per score del college:

```
python init.py all -c -dp mongo -ip localhost 
```
