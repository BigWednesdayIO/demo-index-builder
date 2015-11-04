Temporary/demo index builder for Crateful

```
# setup elasticsearch...

# setup bestbuy data in mongodb
docker run -d -p 27017:27017 --name bestbuy_mongodb -e BESTBUY_API_KEY=<your_api_key> bigwednesdayio/bestbuy_mongodb
docker exec -it bestbuy_mongodb mongo bestbuy --eval 'db.products.createIndex({"categoryPath.id": 1})'
docker exec -it bestbuy_mongodb import.sh

# 5-10 mins later when finished
node index.js
```
