# NFTHype-DataManagementProject
Data Management Project

L’idea che sta dietro alla realizzazione di questo progetto è la seguente: dal momento che gli NFT
sono un fenomeno molto recente e molto discusso in rete, è stato deciso di creare un database
che associa ad ogni collection NFT circa un migliaio (ove possibile) di tweet presi nell’ultima
settimana dal momento dell’inizio della ricerca. L’obiettivo è quello di ottenere delle metriche di
popolarità per ogni progetto NFT per analizzare quanto “hype” ci sia intorno a quel determinato
progetto e analizzare quali possano essere i progetti potenzialmente più soggetti a crescita di
valore (la popolarità è una metrica che influenza pesantemente il valore). Per fare ciò, i dati sono
stati integrati con le API di Twitter, raccogliendo circa 300 mila tweets per le prime 1532
collections NFT considerate. Sono stati scaricati circa 1000 tweets per ogni collezione NFT e, in
seguito all’integrazione dei dataset, sono stati associati tutti i tweets riguardanti una collection
alla collection stessa, aumentandone (in alcuni casi) il numero. Tramite queste operazioni si può
contare il numero di tweets che ogni collezione ha raccolto in un determinato periodo di tempo,
in modo da valutarne la popolarità rispetto ad un’altra che potrebbe aver raccolto meno tweets o
lo stesso numero (circa) in un periodo di tempo più lungo.
