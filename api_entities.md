### 1. Список полей, которые необходимы для витрины: 
   1. `courier_id` - берем из dds.dm_courier
   2. `courier_name` - берем из dds.dm_courier
   3. `settlement_year` - вытаскиваем из dds.dm_timestamp
   4. `settlement_month` - вытаскиваем из dds.dm_timestamp
   5. `orders_count` - COUNT dm_orders
   6. `orders_total_sum` - COUNT по dm_deliveries
   7. `rate_avg` - avg из dm_deliveries
   8. `order_processing_fee` - orders_total_sum * 0.25
   9. `courier_order_sum` - orders_total_sum и rate_avg
   10. `courier_tips_sum` - dm_deliveries.tip_sum
   11. `courier_reward_sum` - courier_order_sum + courier_tips_sum * 0.95


### 2. Список таблиц в слое DDS, из которых вы возьмёте поля для витрины. 
   - dm_order  - таблица уже есть, изменений не будет,
   - dm_timestamp - таблица уже есть, изменений не будет,
   - dm_deliveries - данные из stg.deliverysystem_deliveries, т.е. из метода `/delivery` в источнике
   - dm_courier - данные из stg.deliverysystem_couriers, т.е. из метода `/courier` в источнике
   

### 3. На основе списка таблиц в DDS составьте список сущностей и полей, которые необходимо загрузить из API.  
Из API мы воспользуемся методами `/couriers` и `/deliveries`, метод `/restaurants` нам не нужен, так как эти данные тянутся из системы заказов.


