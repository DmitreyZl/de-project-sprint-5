CREATE TABLE stg.deliverysystem_couriers (
	id serial4 NOT NULL PRIMARY KEY,
	courier_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT stg_couriers_courier_id_uindex UNIQUE (courier_id)
);

CREATE TABLE stg.deliverysystem_deliveries (
	id serial4 NOT NULL PRIMARY KEY,
	delivery_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT stg_deliveries_delivery_id_uindex UNIQUE (delivery_id)
)