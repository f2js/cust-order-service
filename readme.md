# Customer Facing Order Service

This is the service handling creation and fetching of orders for the customer. 

## Status
Currently only in proof-of-concept state. In order to work you need to manually open a SSH connection to the database droplet, and run `~/hbase/bin/hbase thrift start -p 9090 --inforport 9095` to start the thrift interface which the service connects to. The thift interface will end if you close the SSH connection.

## Database 
The service uses HBase as the database. Below is a sketch of the datamodel.

<table>
  <tr>
    <td rowspan="2"></td>
    <td rowspan="2">rowkey</td>
    <td colspan="3">info</td>
    <td colspan="2">ids</td>
    <td colspan="2">addr</td>
    <td colspan="6">ol</td>
  </tr>
  <tr>
    <td>o_id</td>
    <td>o_time</td>
    <td>state</td>
    <td>c_id</td>
    <td>r_id</td>
    <td>c_addr</td>
    <td>r_addr</td>
    <td>1</td>
    <td>2</td>
    <td>3</td>
    <td colspan="3">...</td>
  </tr>
  <tr>
    <td>Content</td>
    <td>**</td>
    <td>*</td>
    <td>DateTime of order creation</td>
    <td>Processing, Pending, Rejected, Accepted, ReadyForPickup, OutForDelivery, Delivered</td>
    <td>Mongo ObjectId</td>
    <td>Mongo ObjectId</td>
    <td>Customer address</td>
    <td>Restaurant address</td>
    <td>menuid:price***</td>
    <td>menuid:price</td>
    <td>menuid:price</td>
    <td>menuid:price</td>
    <td>menuid:price</td>
    <td>menuid:price</td>
  </tr>
  <tr>
    <td>Examples</td>
    <td></td>
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
* sha256 of c_id, r_id, ordertime and all orderlines

** random salt using r_id as seed with o_id appended

*** price in cents/ører