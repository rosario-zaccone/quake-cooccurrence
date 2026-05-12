# Analisi distribuita di co-occorrenza di terremoti

**Corso:** Scalable and Cloud Programming - A.A. 2025/2026  
**Progetto:** implementazione Scala + Apache Spark su Google Cloud Dataproc  
**Repository:** TODO: inserire URL del repository GitHub pubblico

## Obiettivo

Il progetto richiede di individuare, su un dataset di terremoti, la coppia di localita distinte che presenta il massimo numero di co-occorrenze giornaliere. Ogni evento viene normalizzato approssimando latitudine e longitudine alla prima cifra decimale e usando solo la data del timestamp. L'output finale e composto dalla coppia di coordinate piu frequente e dalla lista ordinata delle date in cui le due localita co-occorrono.

L'elaborazione e stata realizzata in Scala con Apache Spark, usando RDD e trasformazioni in stile map-reduce. L'esecuzione sperimentale e stata effettuata su Google Cloud Dataproc con macchine `n2-standard-4`, variando sia il numero di worker sia il numero di partizioni usato dal job.

## Preparazione dei dati

Il programma legge il CSV da Google Cloud Storage tramite Spark SQL e lo converte in un RDD di coppie:

```scala
(date, Coordinate(latRounded, lonRounded))
```

La data viene estratta dai primi 10 caratteri del campo `date`, mentre le coordinate vengono arrotondate a una cifra decimale. Subito dopo la lettura, il dataset viene ripartizionato con:

```scala
repartition(sc.defaultParallelism * partitionMultiplier)
```

Il parametro `partitionMultiplier` consente di confrontare configurazioni con partizionamento pari a `2x`, `3x` e `4x` rispetto al parallelismo di default del cluster.

Prima di generare le coppie di localita, gli eventi con stessa data e stessa cella geografica vengono deduplicati usando un `Set[Coordinate]`. Questo passaggio e necessario per evitare che piu terremoti nella stessa cella e nello stesso giorno producano co-occorrenze duplicate o coppie spurie con coordinate equivalenti.

## Approccio implementativo

L'algoritmo generale e il seguente:

1. lettura del dataset da `gs://<bucket>/dataset.csv`;
2. normalizzazione di data e coordinate;
3. raggruppamento per data e deduplicazione delle coordinate;
4. generazione di tutte le coppie distinte di localita presenti nello stesso giorno;
5. aggregazione per coppia di localita;
6. selezione della coppia con cardinalita massima;
7. ordinamento crescente delle date di co-occorrenza.

Sono state implementate cinque varianti per confrontare l'impatto delle primitive Spark:

| Solver | Caratteristiche principali |
|---|---|
| `Solver1` | baseline con `groupByKey` per data e per coppia; semplice ma piu costoso in shuffle e memoria. |
| `Solver2` | sostituisce il primo `groupByKey` con `aggregateByKey`, riducendo i dati gia lato partizione. |
| `Solver3` | usa `aggregateByKey` e genera le coppie con `combinations(2)`, mantenendo una struttura piu dichiarativa. |
| `Solver4` | usa cicli `while` e `ArrayBuffer` per ridurre overhead nella generazione delle coppie; aggrega le date con `Set`. |
| `Solver5` | genera le coppie dentro `mapPartitions` e usa `treeReduce` per la selezione finale. |

In tutte le versioni la coppia di coordinate viene ordinata in modo deterministico, cosi la coppia `(A, B)` e `(B, A)` viene trattata come la stessa co-occorrenza.

## Ambiente sperimentale

Le prove sono state eseguite su Dataproc con:

| Parametro | Valore |
|---|---|
| Regione | `europe-west1` |
| Tipo macchina | `n2-standard-4` |
| Worker testati | 2, 3, 4 |
| Partizionamento | `2x`, `3x`, `4x` rispetto a `sc.defaultParallelism` |
| Numero run | 3 per combinazione solver/worker/partizioni |

Gli script in `scripts/` automatizzano creazione dei bucket, upload del dataset, creazione del cluster, sottomissione dei job, raccolta dei log e generazione dei grafici. Le metriche aggregate sono salvate in `results/all_results.csv`.

## Risultati

La tabella seguente riporta il tempo medio in secondi su tre run per ciascuna configurazione.

| Solver | Worker | p2x | p3x | p4x |
|---|---:|---:|---:|---:|
| Solver1 | 2 | 576.1 | 553.0 | 340.1 |
| Solver1 | 3 | 639.7 | 403.9 | 317.5 |
| Solver1 | 4 | 588.4 | 414.1 | 258.5 |
| Solver2 | 2 | 592.9 | 555.3 | 349.5 |
| Solver2 | 3 | 649.3 | 404.5 | 307.4 |
| Solver2 | 4 | 631.5 | 418.4 | 235.5 |
| Solver3 | 2 | 698.0 | 651.8 | 423.0 |
| Solver3 | 3 | 732.3 | 478.2 | 395.4 |
| Solver3 | 4 | 716.6 | 477.2 | 323.0 |
| Solver4 | 2 | 1646.8 | 1313.2 | 1061.8 |
| Solver4 | 3 | 1509.5 | 1030.0 | 828.3 |
| Solver4 | 4 | 1323.2 | 950.5 | 626.6 |
| Solver5 | 2 | 1491.9 | 1526.5 | 1050.0 |
| Solver5 | 3 | 1471.6 | 1036.1 | 915.4 |
| Solver5 | 4 | 1256.0 | 918.0 | 732.1 |

![Tempo medio con partizionamento p2x](../results/time_p2x_by_solver_workers.png)

![Speedup con partizionamento p4x](../results/time_p4x_speedup_vs_2_workers.png)

Il miglior tempo assoluto e stato ottenuto da `Solver2` con 4 worker e partizionamento `p4x`, pari a circa **235.5 s**. Con `p3x`, `Solver1` su 3 worker e risultato il piu veloce, con circa **403.9 s**. Con `p2x`, invece, il miglior tempo e `Solver1` su 2 worker, pari a circa **576.1 s**.

## Analisi di scalabilita

L'andamento mostra che aumentare il numero di worker porta benefici soprattutto quando il partizionamento e sufficientemente alto. Con `p4x`, tutti i solver migliorano passando da 2 a 4 worker: `Solver2` raggiunge uno speedup medio di circa **1.48x**, `Solver4` arriva a **1.69x**, e `Solver1` a **1.32x**. Questo indica che, con abbastanza partizioni, il lavoro viene distribuito meglio e il costo della generazione delle coppie viene assorbito in modo piu efficace dal cluster.

Con `p2x` la scalabilita e invece limitata: per i solver piu veloci l'aggiunta di worker non produce miglioramenti stabili e in alcuni casi peggiora i tempi. Questo comportamento e coerente con un numero di task non sufficiente a saturare il cluster e con il costo dello shuffle, che rimane dominante rispetto al guadagno di parallelismo.

`Solver4` e `Solver5`, pur introducendo ottimizzazioni imperative e aggregazioni con `Set`, risultano piu lenti nelle misure complessive. La causa piu probabile e che la fase dominante non sia solo il costo locale di generazione delle coppie, ma anche la quantita di dati intermedi e lo shuffle necessario per aggregare le date per coppia. Le ottimizzazioni locali non compensano quindi il maggiore overhead complessivo rispetto alle versioni piu semplici.

## Conclusioni

La soluzione soddisfa i requisiti della traccia: usa Scala e Spark su Dataproc, normalizza date e coordinate, rimuove i duplicati per giorno e cella geografica, individua la coppia di localita con massimo numero di co-occorrenze e restituisce le date ordinate.

Dal confronto sperimentale emerge che le versioni piu efficaci sono `Solver1` e `Solver2`. `Solver1` e competitivo nonostante l'uso di `groupByKey`, mentre `Solver2` e la scelta migliore nella configurazione piu scalabile testata, cioe 4 worker e partizionamento `p4x`. Il partizionamento ha un impatto rilevante: configurazioni troppo basse non sfruttano bene i worker aggiuntivi, mentre `p4x` offre il miglior compromesso tra parallelismo e costo di shuffle.

