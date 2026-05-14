# Analisi distribuita di co-occorrenza di terremoti

**Corso:** Scalable and Cloud Programming - A.A. 2025/2026  
**Progetto:** implementazione Scala + Apache Spark su Google Cloud Dataproc  
**Repository:** TODO:

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
| Ripetizioni sperimentali | diverse run per combinazione solver/worker/partizioni |

Gli script in `scripts/` automatizzano creazione dei bucket, upload del dataset, creazione del cluster, sottomissione dei job, raccolta dei log e generazione dei grafici. Le metriche aggregate sono salvate in `results/all_results.csv`.

## Risultati

La tabella seguente riporta il tempo medio in secondi calcolato su diverse run per ciascuna configurazione.

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

Il primo risultato rilevante e che il partizionamento incide piu del semplice aumento dei worker. Per i solver piu efficienti, passare da `p2x` a `p4x` riduce drasticamente il tempo di esecuzione: con 4 worker, `Solver1` passa da **588.4 s** a **258.5 s**, mentre `Solver2` passa da **631.5 s** a **235.5 s**. In termini relativi, questo corrisponde a un miglioramento di circa **2.28x** per `Solver1` e **2.68x** per `Solver2`. Il risultato e coerente con la natura del problema: la generazione e aggregazione delle coppie produce molti dati intermedi, quindi un numero maggiore di partizioni permette di distribuire meglio sia il calcolo sia lo shuffle.

Il secondo aspetto importante e che il miglior solver non e sempre lo stesso in tutte le configurazioni. `Solver1` risulta leggermente migliore nelle configurazioni con meno parallelismo effettivo, mentre `Solver2` diventa il migliore nella configurazione piu favorevole alla scalabilita. Questo suggerisce che l'uso di `aggregateByKey` porta beneficio soprattutto quando il cluster ha abbastanza task da eseguire in parallelo: la riduzione locale dei dati prima dello shuffle diventa piu utile quando il lavoro e distribuito su piu partizioni.

## Analisi di scalabilita

L'andamento mostra che aumentare il numero di worker porta benefici soprattutto quando il partizionamento e sufficientemente alto. Con `p4x`, tutti i solver migliorano passando da 2 a 4 worker: `Solver2` raggiunge uno speedup di circa **1.48x**, `Solver4` arriva a **1.69x**, `Solver5` a **1.43x** e `Solver1` a **1.32x**. Questo indica che, con abbastanza partizioni, il lavoro viene distribuito meglio e il costo della generazione delle coppie viene assorbito in modo piu efficace dal cluster.

| Solver | Speedup p2x, 2->4 worker | Speedup p3x, 2->4 worker | Speedup p4x, 2->4 worker |
|---|---:|---:|---:|
| Solver1 | 0.98x | 1.34x | 1.32x |
| Solver2 | 0.94x | 1.33x | 1.48x |
| Solver3 | 0.97x | 1.37x | 1.31x |
| Solver4 | 1.24x | 1.38x | 1.69x |
| Solver5 | 1.19x | 1.66x | 1.43x |

Con `p2x` la scalabilita e invece limitata: per i solver piu veloci l'aggiunta di worker non produce miglioramenti stabili e in alcuni casi peggiora i tempi. Questo comportamento e coerente con un numero di task non sufficiente a saturare il cluster e con il costo dello shuffle, che rimane dominante rispetto al guadagno di parallelismo.

La tabella degli speedup evidenzia anche un limite tipico delle applicazioni distribuite: aggiungere risorse non garantisce automaticamente tempi migliori. Nei casi `p2x`, `Solver1`, `Solver2` e `Solver3` hanno speedup inferiori a 1 passando da 2 a 4 worker, quindi l'esecuzione con piu worker e piu lenta. Questo non significa che Spark non stia parallelizzando, ma che il parallelismo disponibile non e sufficiente a compensare overhead di scheduling, serializzazione e shuffle. Quando invece il partizionamento sale a `p3x` e soprattutto a `p4x`, lo speedup torna positivo e il cluster viene sfruttato in modo piu efficace.

## Confronto tra solver

Nel confronto complessivo, `Solver1` e `Solver2` formano il gruppo piu efficiente. La media globale delle configurazioni e molto vicina: circa **454.6 s** per `Solver1` e **460.5 s** per `Solver2`. Tuttavia, il dato piu interessante non e la media aggregata, ma il comportamento nella configurazione migliore: con 4 worker e `p4x`, `Solver2` e circa il **9%** piu veloce di `Solver1` (**235.5 s** contro **258.5 s**). Questo rende `Solver2` la scelta piu convincente quando l'obiettivo e sfruttare al meglio il cluster.

`Solver3` rimane piu lento di `Solver1` e `Solver2`, ma mantiene un andamento simile: migliora sensibilmente con `p4x` e scala in modo abbastanza regolare aumentando i worker. La differenza rispetto ai primi due solver e probabilmente dovuta al costo aggiuntivo della generazione delle combinazioni e alla minore efficienza delle strutture intermedie usate nella pipeline.

`Solver4` e `Solver5`, pur introducendo ottimizzazioni imperative, `mapPartitions`, `treeReduce` e aggregazioni con `Set`, risultano nettamente piu lenti nelle misure complessive. In media sono circa **2.5-2.6x** piu lenti di `Solver1`. Questo risultato e significativo perche mostra che ottimizzare localmente una singola fase non basta se il collo di bottiglia principale resta la quantita di dati intermedi e lo shuffle necessario per aggregare le date per coppia. In questo caso le versioni piu semplici, che lasciano a Spark una pipeline piu lineare, risultano piu efficaci delle versioni piu imperative.

Dal punto di vista progettuale, il risultato conferma una lezione importante: in Spark la scelta delle trasformazioni deve ridurre il traffico distribuito prima ancora di ridurre il costo CPU locale. Per questo `Solver2`, che usa `aggregateByKey` per limitare i dati gia a livello di partizione, e la variante piu promettente nella configurazione con maggiore parallelismo.

## Conclusioni

La soluzione soddisfa i requisiti della traccia: usa Scala e Spark su Dataproc, normalizza date e coordinate, rimuove i duplicati per giorno e cella geografica, individua la coppia di localita con massimo numero di co-occorrenze e restituisce le date ordinate.

Dal confronto sperimentale emerge che le versioni piu efficaci sono `Solver1` e `Solver2`. `Solver1` e competitivo nonostante l'uso di `groupByKey`, mentre `Solver2` e la scelta migliore nella configurazione piu scalabile testata, cioe 4 worker e partizionamento `p4x`. Il partizionamento ha un impatto rilevante: configurazioni troppo basse non sfruttano bene i worker aggiuntivi, mentre `p4x` offre il miglior compromesso tra parallelismo e costo di shuffle.

La configurazione consigliata e quindi `Solver2` con 4 worker e partizionamento `p4x`. Questa scelta non e solo quella con il tempo minimo, ma anche quella piu coerente con il modello di esecuzione distribuita: aumenta il parallelismo disponibile, riduce i dati prima dello shuffle e sfrutta meglio le risorse del cluster rispetto alle alternative testate.
