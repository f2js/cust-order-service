# Customer Facing Order Service

This is the service handling creation and fetching of orders for the customer. 

## Status
[![CircleCI](https://dl.circleci.com/status-badge/img/gh/f2js/cust-order-service/tree/main.svg?style=svg&circle-token=a610a02cf3ad0a9fc3c877a2d99a162127b93ed5)](https://dl.circleci.com/status-badge/redirect/gh/f2js/cust-order-service/tree/main)

[![CircleCI](https://dl.circleci.com/insights-snapshot/gh/f2js/cust-order-service/main/build-deploy-master/badge.svg?window=30d&circle-token=9dfa94882002edd431767c1c3624cd4d4e9c04f9)](https://app.circleci.com/insights/github/f2js/cust-order-service/workflows/build-deploy-master/overview?branch=main&reporting-window=last-30-days&insights-snapshot=true)

## REST API
### POST /create
Creates an order. Should be only accessible through the legacy application, by having the API Gateway ignore this endpoint. 

#### Request Body:
- c_id (String): The ID of the customer, as found in the User Database.
- r_id (String): The ID of the restaurant, as found in the Restaurant Database.
- cust_addr (String): The address of the customer.
- rest_addr (String): The address of the restaurant.
- postal_code (Unsinged Int): The postal code of the customer.
- orderlines (Array): The lines in the order: 
  - item_num (Unsinged Int): The order item number on the menu of the restaurant.
  - price (Unsigned Int): The price of the item, in cents/ører.
 
 #### Response:
 - 200 OK: The order was successfully created.
 - 400 Bad Request: The request body was missing or invalid.
 - 500 Internal Server Error: An error occurred on the server side.
 
 ### GET /order/{id}
 Gets an order by the id. Should be rewritten to also take a customer id, and only return the order if it is owned by the given customer. This id should be set by the API Gateway, and shouldn't be settable by the client.
 
 #### Response
 - 200 OK: The order was successfully found. The response body contains the order in JSON format. 
 - 404 Not Found: The order was not found.
 - 500 Internal Server Error: An error occurred on the server side.

### GET /cust/{id}
Gets all orders for a given customer. Does not fetch orderlines.

#### Response
- 200 OK: The orders were successfully found. The response body contains a list of the orders for the given customer.
- 404 Not Found: There was no orders found for the customer.
- 500 Internal Server Error: An error occurred on the server side.

## Database 
The service uses HBase as the database. Below is a sketch of the datamodel.

<table>
  <tr>
    <td><i>Column Family</i></td>
    <td rowspan="2"><b>rowkey</b></td>
    <td colspan="3"><b>info</b></td>
    <td colspan="2"><b>ids</b></td>
    <td colspan="2"><b>addr</b></td>
    <td colspan="6"><b>ol</b></td>
  </tr>
  <tr>
    <td><i>Column</i></td>
    <td><i><b>o_time</b></i></td>
    <td><i><b>state</b></i></td>
    <td><i><b>c_id</b></i></td>
    <td><i><b>r_id</b></i></td>
    <td><i><b>c_addr</b></i></td>
    <td><i><b>r_addr</b></i></td>
    <td><i><b>1</b></i></td>
    <td><i><b>2</b></i></td>
    <td><i><b>3</b></i></td>
    <td colspan="3"><i><b>...</b></i></td>
  </tr>
  <tr>
    <td><i>Content</i></td>
    <td>*</td>
    <td>DateTime of order creation</td>
    <td>Processing, Pending, Rejected, Accepted, ReadyForPickup, OutForDelivery, Delivered</td>
    <td>Mongo ObjectId</td>
    <td>Mongo ObjectId</td>
    <td>Customer address</td>
    <td>Restaurant address</td>
    <td>menuid:price**</td>
    <td>-||-</td>
    <td>-||-</td>
    <td>-||-</td>
    <td>-||-</td>
    <td>-||-</td>
  </tr>
  <tr>
    <td><i>Examples</i></td>
    <td></td>
    <td>2022-25-08 13:48:25</td>
    <td>Pending</td>
    <td>"507f1f77bcf86cd799439011"</td>
    <td>"507f191e810c19729de860ea"</td>
    <td>Lyngvej 2, 2800 Lyngby</td>
    <td>Lyngvej 2, 2800 Lyngby</td>
    <td>25:70</td>
    <td>12:60</td>
    <td>12:60</td>
    <td>5:52</td>
    <td>3:10</td>
    <td>1:15</td>
  </tr>
</table>
* sha256 of c_id, r_id, ordertime and all orderlines with random salt using r_id as seed appended to front, to make searching easier for restaurants

** price in cents/ører

## Kafka Events
### Produced
#### OrderCreated
This event is produced when an order is created. It contains all the contents of the order, as JSON. 
##### Body
- o_id (String): The ID of the order in the order-database. 
- c_id (String): The ID of the customer, as found in the User Database.
- r_id (String): The ID of the restaurant, as found in the Restaurant Database.
- ordertime (String): The time which the order was created.
- cust_addr (String): The address of the customer.
- rest_addr (String): The address of the restaurant.
- postal_code (Unsinged Int): The postal code of the customer.
- state (String): The state of the order. Possible values are:
  - Processing: The order payment is still being processes.
  - Pending: The order is pending acceptance from the restaurant.
  - Rejected: The order was rejected by the restaurant.
  - Accepted: The order was accepted by the restaurant, and has begun cooking.
  - ReadyForPickup: The order is waiting for the courier to pick it up.
  - OutForDelivery: The order is in the process of being delivered.
  - Delivered: The order has been delivered to the customer. 
- orderlines (Array): The lines in the order: 
  - item_num (Unsinged Int): The order item number on the menu of the restaurant.
  - price (Unsigned Int): The price of the item, in cents/ører.