Feature: Create Order

Scenario: Creating Order and Checking Database
Given we have an order database
When we create a new order
Then the order is created in the database

Scenario: Creating Order and receiving row-id
Given we have an order database
When we create a new order
Then the row-id is returned