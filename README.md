# PUBSUB - AVRO serialization and desarialization

-python consumer'ı çalıştırmak için
```
python avroproducer.py group_id sub_topic_name pub-topic_name
```

-python producer'ı çalıştırmak için
-python dosyaismi.py pub_topic_name

-topicleri istediğiniz isimlerle oluşturabilirsiniz. 1 vehicle topic, 3 violation topic

-MongoDB controller oluştururken database'e veri gönderecek topic seçilir.
-doldurulması gereken kısımlar aşağıda belirtilmiştir.

-key converter class = org.apache.kafka.connect.storage.StringConverter
-value converter class = io.confluent.connect.avro.AvroConverter
-mongoDB connection uri = mongodb+srv://new_user_41:su5FuHgMoWcsq4qV@cluster0.uj9vf.mongodb.net/
-mongoDB database name = Violation
-mongoDB database name = violation1 or violation2 or violation3

-En aşağından "add a property" kısmından aşağıdaki property eklenmeli.
-value.converter.schema.registry.url =  http://schema-registry:8081
