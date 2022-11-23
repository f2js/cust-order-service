extern crate order_service;
use cucumber::{given, then, when, World, Parameter};

#[derive(World, Debug, Default, Clone)]
pub struct State {
    input: Option<()>,
    output: Option<()>,
}

fn main() {
    futures::executor::block_on(State::run("features/"));
}