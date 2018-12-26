# RedisTemplateHelper

RedisTemplateHelper是一个基于Spring RedisTemplate的一个加强工具类，对RedisTemplate的各项操作进行了增强，其主要特性：

- 1、对Redis各项批量操作进行了增强，支持多种数据结构的单次操作与批量操作
- 2、对RedisTemplate进行了扩展，支持线程内库号的切换
- 3、支持JavaBean的到Redis存储的JSON转换，也支持Redis存储数据到JavaBean的反向转换
