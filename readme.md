# Customer Facing Order Service

This is the service handling creation and fetching of orders for the customer. 

## Status
Currently only in proof-of-concept state. In order to work you need to manually open a SSH connection to the database droplet, and run `~/hbase/bin/hbase thrift start -p 9090 --inforport 9095`

## Database 
The service uses HBase as the database. Below is a sketch of the datamodel.

<table>
  <tr>
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
    <td>**</td>
    <td>*</td>
    <td>2022-25-08 13:48:25</td>
    <td>Pending</td>
    <td>Mongo ObjectId</td>
    <td>Mongo ObjectId</td>
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