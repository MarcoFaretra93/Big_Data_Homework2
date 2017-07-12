# Big_Data_Homework_2
# Marco Faretra	Gabriele Marini

## Requisiti

I requisiti per l'avvio del progetto in locale sono:
* [Dataset dei giocatori](https://mega.nz/#F!R1cCRJrB!R_yciIq-MSbRIozaVVy-Zw)
* Python 2.7
* Un'installazione di Hadoop (versione utilizzata: 2.8)
* Un'installazione di Spark (versione utilizzata: 2.1.1, la versione 2.1.0 presentava delle anomalie nella creazione della configurazione)
* Un'installazione di MongoDB
* Un'installazione di Redis
* (Optional) Docker

I file relativi al dataset dei giocatori devono essere scaricati e posizionati nella root del progetto.

Per semplificare l'installazione e l'utilizzo di Redis e MongoDB, si è utilizzato Docker, attraverso i seguenti comandi:

* Per MongoDB:

```
docker run -p 27017:27017 --name mongo -d mongo
```

Comando che crea un container di nome "mongo", con l'installazione del database al suo interno.

* Per Redis:

```
docker run -p 6379:6379 --name redis -d redis
```

Comando che crea un container di nome "redis", con l'installazione del key-value store al suo interno.

L'opzione -p permette il forward delle porte tra host:container, in questo modo i due database sono accessibili mediante localhost.

### Dipendenze 

* [Connettore mongo-hadoop](https://github.com/mongodb/mongo-hadoop)

Seguire la guida d'installazione, in particolare la sezione relativa a [spark](https://github.com/mongodb/mongo-hadoop/wiki/Spark-Usage)

## Dati 

I giocatori presenti nel dataset sono stati suddivisi in 7 categorie, per ognuna di queste si è calcolato uno score per ogni giocatore. Per quanto riguarda i college, il punteggio di un college è definito come la sommatoria dello score dei giocatori appartenenti a quest'ultimo. 
Dato che il ranking è coerente solamente all'interno della stessa categoria, per ottenere lo score complessivo di un college viene effettuata, per ogni categoria, una normalizzazione dello score ottenuto in maniera da scalarlo tra 0 e 100, dando di fatto lo stesso peso ad ogni categoria ai fini del computo dello score finale.

Le categorie di giocatori individuate sono:
 
* "plus/minus", giocatori apportano un contributo alla squadra anche senza che questo sia evidente nelle statistiche
* "all-around", giocatori capaci di essere efficaci in vari aspetti del gioco
* "tiratori da 2 punti", tiratori eccellenti soprattuto da dentro l'arco dei 3 punti
* "tiratori da 3 punti", tiratori eccellenti soprattuto fuori l'arco dei 3 punti
* "difensori", giocatori capaci di difendere il canestro in maniera eccellente
* "attaccanti", giocatori capaci di segnare svariate volte con alta efficienza
* "rimbalzisti", giocatori capaci di catturare sia rimbalzi difensivi che offensivi

## Esecuzione 

È possibile vedere la documentazione completa dei parametri attraverso il comando: 

```
python init.py --help
```

### Popolamento dei DB

Per popolare MongoDB e Redis, basta avviare lo script python, init.py con il parametro populate e specificando l'ip della macchina sul quale sono installati i databases.  

```
python init.py populate -ip localhost
```

Successivamente è possibile avviare l'esecuzione di spark utilizzando una delle seguenti keyword:

* attackers
* defenders
* 2_point_shooters
* 3_point_shooters
* rebounders
* plus_minus
* all_around

Specificandone la categoria dei giocatori è possibile quindi lanciare le esecuzioni, di seguito alcuni esempi:

* Restituire tutti gli attaccanti con score annesso (parallelizzando i dati utilizzando MongoDB e il suo connettore):

```
python init.py attackers -dp mongo -ip localhost
```

* Restituire i college con score annesso, sulla base della categoria richiesta (in questo caso i difensori):

```
python init.py defenders -dp mongo -ip localhost -c 
```

* Se si vuole avviare l'esecuzione in ambiente distribuito, basta aggiungere l'opzione "--dist":

```
python init.py defenders -dp mongo -ip localhost -c --dist
```

* È possibile anche parallelizzare i dati utilizzando Redis: 

```
python init.py attackers -dp redis -ip localhost
```

* Per lanciare una volta sola il calcolo su tutte le categorie, basta inserire come categoria "all": 

* Giocatori:

```
python init.py all -dp mongo -ip localhost 
``` 

* College:

```
python init.py all -c -dp mongo -ip localhost 
```
