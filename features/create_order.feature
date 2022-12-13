Feature: Create Order

Scenario: Creating Order and Checking Database
Given we have an order database
When we create a new order
Then the order is created in the database