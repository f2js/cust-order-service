use std::{ops::{Deref, DerefMut}, str::FromStr};


use actix_web::{web};
use chrono::{Utc, DateTime, NaiveDateTime};
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use sha2::{Sha256, Digest};

const SERIALIZE_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S.%f %Z";

// Types
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateOrder {
    pub c_id: String,
    pub r_id: String,
    pub cust_addr: String,
    pub rest_addr: String,
    pub orderlines: Vec<Orderline>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderInfo {
    pub o_id: String,
    pub ordertime: String,
    pub state: OrderState,
    pub r_id: String,
    pub c_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Order {
    pub o_id: String,
    pub c_id: String,
    pub r_id: String,
    pub ordertime: String,
    pub orderlines: Vec<Orderline>,
    pub state: OrderState,
    pub cust_addr: String,
    pub rest_addr: String,
}
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Orderline {
    pub item_num: u32,
    pub price: u32,
}
#[derive(Debug, Default, Clone)]
pub struct OrderBuilder {
    pub o_id: Option<String>,
    pub c_id: Option<String>,
    pub r_id: Option<String>,
    pub ordertime: Option<String>,
    pub state: Option<String>,
    pub cust_addr: Option<String>,
    pub rest_addr: Option<String>,
    pub orderlines: Vec<Orderline>,
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum OrderState {
    Processing,
    Pending,
    Rejected,
    Accepted,
    ReadyForPickup,
    OutForDelivery,
    Delivered,
}
#[derive(Debug, Clone)]
pub struct FormattedDateTime(DateTime<Utc>);

// Impls
impl Order {
    pub fn new (orderlines: Vec<Orderline>, cust_addr: String, rest_addr: String, c_id: String, r_id: String) -> Self {
        let ordertime = FormattedDateTime::new().to_rfc3339();
        Self {
            o_id: to_u32(&Order::hash(&c_id, &r_id, &ordertime.to_string(), &orderlines)).to_string(),
            c_id,
            r_id,
            ordertime,
            orderlines,
            state: OrderState::Pending,
            cust_addr,
            rest_addr,
        }
    }

    pub fn hash(c_id: &str, r_id: &str, ordertime: &str, orderlines: &Vec<Orderline>) -> [u8; 32]{
        let mut hasher = Sha256::new();
        hasher.update(c_id);
        hasher.update(r_id);
        hasher.update(ordertime);
        for ol in orderlines {
            hasher.update(ol.to_string());
        }
        hasher.finalize().into()
    }

    pub fn build(builder: OrderBuilder) -> Option<Self> {
        let orderstate = match OrderState::from_str(&builder.state?) {
            Ok(s)=> s,
            Err(_) => return None,
        };
        Some(Self {
            o_id: builder.o_id?,
            c_id: builder.c_id?,
            r_id: builder.r_id?,
            cust_addr: builder.cust_addr?,
            rest_addr: builder.rest_addr?,
            state: orderstate,
            ordertime: builder.ordertime?,
            orderlines: builder.orderlines,
        })
    }
}

impl From<web::Json<CreateOrder>> for Order {
    fn from(params: web::Json<CreateOrder>) -> Self {
        Self::new(
            params.orderlines.clone(),
            params.cust_addr.clone(),
            params.rest_addr.clone(),
            params.c_id.clone(),
            params.r_id.clone(),
        )
    }
}

impl FormattedDateTime {
    fn new() -> Self {
        Self(Utc::now())
    }
}

impl Serialize for FormattedDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // If you implement `Deref`, then you don't need to add `.0`
        let s = format!("{}", self.format(SERIALIZE_FORMAT));
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for FormattedDateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(s.as_str(), SERIALIZE_FORMAT)
            .map_err(serde::de::Error::custom)
            .map(|x| {
                let now = Utc::now();
                let date: DateTime<Utc> = DateTime::from_utc(x, now.offset().clone());
                Self(date)
                // or
                // date.into()
            })
    }
}

impl Deref for FormattedDateTime {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FormattedDateTime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<DateTime<Utc>> for FormattedDateTime {
    fn from(date: DateTime<Utc>) -> Self {
        Self(date)
    }
}

impl FormattedDateTime {
    pub fn parse_from_str(str: &str) -> Result<Self, chrono::ParseError> {
        let s = DateTime::parse_from_str(&str, SERIALIZE_FORMAT)?;
        Ok(Self(s.into()))
    }
}

impl Into<DateTime<Utc>> for FormattedDateTime {
    fn into(self) -> DateTime<Utc> {
        self.0
    }
}

impl std::fmt::Display for OrderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderState::Processing => write!(f, "Processing"),
            OrderState::Pending => write!(f, "Pending"),
            OrderState::Rejected => write!(f, "Rejected"),
            OrderState::Accepted => write!(f, "Accepted"),
            OrderState::ReadyForPickup => write!(f, "ReadyForPickup"),
            OrderState::OutForDelivery => write!(f, "OutForDelivery"),
            OrderState::Delivered => write!(f, "Delivered"),
        }
    }
}

impl std::str::FromStr for OrderState { 
    type Err = ();
    fn from_str(input: &str) -> Result<OrderState, Self::Err> {
        match input {
            "Processing" => Ok(OrderState::Processing),
            "Pending" => Ok(OrderState::Pending),
            "Rejected" => Ok(OrderState::Rejected),
            "Accepted" => Ok(OrderState::Accepted),
            "ReadyForPickup" => Ok(OrderState::ReadyForPickup),
            "OutForDelivery" => Ok(OrderState::OutForDelivery),
            "Delivered" => Ok(OrderState::Delivered),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for Orderline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}:{:?}", self.item_num, self.price)
    }
}

impl FromStr for Orderline {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (item, price) = match s.split_once(':') {
            Some(v) => v,
            None => ("a", "a"), //Hack to make error thrown, remember to change
        };
        let item = item.parse::<u32>()?;
        let price = price.parse::<u32>()?;
        Ok(Self {
            item_num: item, 
            price,
        })
    }
}

impl OrderInfo {
    pub fn build(builder: OrderBuilder) -> Option<Self> {
        let orderstate = match OrderState::from_str(&builder.state?) {
            Ok(s)=> s,
            Err(_) => return None,
        };
        Some(Self {
            o_id: builder.o_id?,
            r_id: builder.r_id?,
            state: orderstate,
            ordertime: builder.ordertime?,
            c_id: builder.c_id?,
        })
    }
}

fn to_u32(slice: &[u8]) -> u32 {
    slice.iter().fold((0,1),|(acc,mul),&bit|(acc+(mul*(1&bit as u32)),mul.wrapping_add(mul))).0
}