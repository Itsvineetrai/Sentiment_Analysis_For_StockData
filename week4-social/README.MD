1. TWITTER:
cd week4-social\twitter-producer
docker build -t twitter-producer .
docker run -it --env-file .\..\..\infra\env.kafka --network infra_default twitter-producer

2.Reddit

cd week4-social\reddit-producer
docker build -t reddit-producer .
docker run --env-file ..\..\infra\env.kafka --network infra_default reddit-producer

3. Yahoo
cd week4-social\yahoo-producer
docker build -t yahoo-producer .
docker run --env-file ..\..\infra\env.kafka --network infra_default yahoo-producer

4.Fianance
cd week4-social\finance-news-producer
docker build -t finance-producer .
docker run --env-file ..\..\infra\env.kafka --network infra_default finance-producer
