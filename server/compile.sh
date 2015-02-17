cd common; mvn clean install || exit 1
cd ..
cd gateway; mvn clean package || exit 1
cd ..
cd simbastore; mvn clean package || exit 1
cd ..
