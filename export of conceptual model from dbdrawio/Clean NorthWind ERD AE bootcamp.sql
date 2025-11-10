CREATE TABLE `dim_customer` (
  `customer_key` int PRIMARY KEY AUTO_INCREMENT,
  `company` varchar(255),
  `contact_name` varchar(255),
  `city` varchar(255),
  `country_region` varchar(255)
);

CREATE TABLE `dim_employee` (
  `employee_key` int PRIMARY KEY AUTO_INCREMENT,
  `first_name` varchar(255),
  `last_name` varchar(255),
  `title` varchar(255),
  `city` varchar(255),
  `country_region` varchar(255)
);

CREATE TABLE `dim_product` (
  `product_key` int PRIMARY KEY AUTO_INCREMENT,
  `product_name` varchar(255),
  `category` varchar(255),
  `standard_cost` decimal,
  `list_price` decimal,
  `discontinued` bool
);

CREATE TABLE `dim_supplier` (
  `supplier_key` int PRIMARY KEY AUTO_INCREMENT,
  `company` varchar(255),
  `city` varchar(255),
  `country_region` varchar(255)
);

CREATE TABLE `dim_date` (
  `date_key` int PRIMARY KEY,
  `full_date` date,
  `day` int,
  `month` int,
  `year` int
);

CREATE TABLE `fact_sales` (
  `sales_key` int PRIMARY KEY AUTO_INCREMENT,
  `order_id` int,
  `customer_key` int,
  `employee_key` int,
  `product_key` int,
  `supplier_key` int,
  `date_key` int,
  `quantity` int,
  `unit_price` decimal,
  `discount` decimal,
  `total_sales` decimal,
  `taxes` decimal,
  `shipping_fee` decimal
);

CREATE TABLE `dim_po_customer` (
  `customer_key` int PRIMARY KEY AUTO_INCREMENT,
  `company` varchar(255),
  `contact_name` varchar(255),
  `city` varchar(255),
  `country_region` varchar(255)
);

CREATE TABLE `dim_po_employee` (
  `employee_key` int PRIMARY KEY AUTO_INCREMENT,
  `first_name` varchar(255),
  `last_name` varchar(255),
  `title` varchar(255),
  `city` varchar(255),
  `country_region` varchar(255)
);

CREATE TABLE `dim_po_product` (
  `product_key` int PRIMARY KEY AUTO_INCREMENT,
  `product_name` varchar(255),
  `category` varchar(255),
  `standard_cost` decimal,
  `list_price` decimal
);

CREATE TABLE `dim_po_supplier` (
  `supplier_key` int PRIMARY KEY AUTO_INCREMENT,
  `company` varchar(255),
  `city` varchar(255),
  `country_region` varchar(255)
);

CREATE TABLE `dim_po_date` (
  `date_key` int PRIMARY KEY,
  `full_date` date,
  `day` int,
  `month` int,
  `year` int
);

CREATE TABLE `fact_purchase_orders` (
  `po_key` int PRIMARY KEY AUTO_INCREMENT,
  `purchase_order_id` int,
  `supplier_key` int,
  `employee_key` int,
  `product_key` int,
  `customer_key` int,
  `date_key` int,
  `quantity` int,
  `unit_cost` decimal,
  `shipping_fee` decimal,
  `taxes` decimal,
  `total_cost` decimal
);

CREATE TABLE `dim_inventory_product` (
  `product_key` int PRIMARY KEY AUTO_INCREMENT,
  `product_name` varchar(255),
  `category` varchar(255)
);

CREATE TABLE `dim_inventory_supplier` (
  `supplier_key` int PRIMARY KEY AUTO_INCREMENT,
  `company` varchar(255),
  `city` varchar(255)
);

CREATE TABLE `dim_inventory_date` (
  `date_key` int PRIMARY KEY,
  `full_date` date,
  `month` int,
  `year` int
);

CREATE TABLE `fact_inventory` (
  `inventory_key` int PRIMARY KEY AUTO_INCREMENT,
  `product_key` int,
  `supplier_key` int,
  `date_key` int,
  `transaction_type` varchar(255),
  `quantity` int
);

ALTER TABLE `fact_sales` ADD FOREIGN KEY (`customer_key`) REFERENCES `dim_customer` (`customer_key`);

ALTER TABLE `fact_sales` ADD FOREIGN KEY (`employee_key`) REFERENCES `dim_employee` (`employee_key`);

ALTER TABLE `fact_sales` ADD FOREIGN KEY (`product_key`) REFERENCES `dim_product` (`product_key`);

ALTER TABLE `fact_sales` ADD FOREIGN KEY (`supplier_key`) REFERENCES `dim_supplier` (`supplier_key`);

ALTER TABLE `fact_sales` ADD FOREIGN KEY (`date_key`) REFERENCES `dim_date` (`date_key`);

ALTER TABLE `fact_purchase_orders` ADD FOREIGN KEY (`customer_key`) REFERENCES `dim_po_customer` (`customer_key`);

ALTER TABLE `fact_purchase_orders` ADD FOREIGN KEY (`employee_key`) REFERENCES `dim_po_employee` (`employee_key`);

ALTER TABLE `fact_purchase_orders` ADD FOREIGN KEY (`product_key`) REFERENCES `dim_po_product` (`product_key`);

ALTER TABLE `fact_purchase_orders` ADD FOREIGN KEY (`supplier_key`) REFERENCES `dim_po_supplier` (`supplier_key`);

ALTER TABLE `fact_purchase_orders` ADD FOREIGN KEY (`date_key`) REFERENCES `dim_po_date` (`date_key`);

ALTER TABLE `fact_inventory` ADD FOREIGN KEY (`product_key`) REFERENCES `dim_inventory_product` (`product_key`);

ALTER TABLE `fact_inventory` ADD FOREIGN KEY (`supplier_key`) REFERENCES `dim_inventory_supplier` (`supplier_key`);

ALTER TABLE `fact_inventory` ADD FOREIGN KEY (`date_key`) REFERENCES `dim_inventory_date` (`date_key`);

ALTER TABLE `fact_sales` ADD FOREIGN KEY (`product_key`) REFERENCES `fact_sales` (`quantity`);
