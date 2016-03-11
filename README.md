# StudentScore

    1. 首先要自定义Score这个类，实现 WritableComparable<Object>这个接口，每一个作为key的类型都会实现这个类，这个类的功能是  
   
   既实现了序列化和反序列化，又实现了比较大小。  
   
    2. 自定义输入格式inputformat。 对输入进行分区，然后读每一个分区。
