#![allow(unused_variables)]
#![allow(dead_code)]

use std::env;

//extern crate rand;

//use rand::{thread_rng, Rng};

// fn incr(value: i32) -> i32 {
//     value+1
// }

// fn foo<'a>(x: &'a str, y: &'a str) -> &'a str {
//     if random() % 2 == 0 {x} else {y}
// }

// fn foo(i: i32) -> Option<i32> {
//     if i > 0 { Some(i+1) } else { None }
// }

// fn function_with_error(i: i32) -> Result<u64, String> {
//     if i > 0 {
//         return Err("bad things happened!".to_string());
//     }
//     Ok(255)
// }
    
// fn get_an_optional_value() -> Option<&'static str> {

//     //if the optional value is not empty
//     if false {
//         return Some("abc");
//     }

//     //else
//     None
// }

// struct Example  
// {  
//     a : i32,  
// }

// impl Drop for Example  
// {  
//     fn drop(&mut self)  
//     {  
//         println!("Dropping the instance of Example with data : {}", self.a);  
//     }  
// }  

use std::io::prelude::*;
use std::net::TcpStream;

use std::net::TcpListener;

use std::str::FromStr;
use std::num::ParseIntError;

#[derive(Debug, PartialEq)]
struct Point {
    x: i32,
    y: i32
}

impl FromStr for Point {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let xy: Vec<&str> = s.trim_matches(|p| p == '(' || p == ')').split(',').collect();
        let x = xy[0].parse::<i32>()?;
        let y = xy[1].parse::<i32>()?;
        Ok(Point{x: x, y: y})
    }
}

fn main() {
    
    let p = Point::from_str("(1,2)");
    assert_eq!(p.unwrap(), Point{x:1, y:2});
}

fn main2() -> std::io::Result<()> {

    {
        // let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        // match listener.accept() {
        //     Ok((_socket, addr)) => println!("new client: {:?}", addr),
        //     Err(e) => println!("couldn't get client: {:?}", e),
        // }

        // println!("------------done 0");
        // std::thread::sleep(std::time::Duration::from_millis(3000));
        // println!("------------done 1");
    }
    
    let mut stream = TcpStream::connect("47.106.208.207:60001")?;
    
    stream.write(&[8])?;
    stream.write(&[0])?;
    stream.write(&[0;6])?;
    
    let mut head = [0; 2];
    let mut body = [0; 128];
    let len: u16;
    
    let mut cnt = stream.read(&mut head)?;
    if cnt == 2 {
        len = head[0] as u16 | ((head[1] as u16) << 8);
        println!("len:{}", len);
        cnt = stream.read(&mut body)?;
        println!("cnt:{}", cnt);
        for i in 0..cnt {
            print!("{} ", body[i]);
        }
        println!("");
        let bodystr = String::from_utf8(body[0..cnt].to_vec()).unwrap();
        println!("bodystr: {}", bodystr);
    }
    
    //stream.read(&mut [0; 128])?;
    
    Ok(())
} // the stream is closed here



fn main1() {

    
    
    // let a1 = Example{a: 10};  
    // let b1 = Example{a: 20};  
    // println!("Instances of Example type are created");  

    // return;
    
    // let x = String::from("abc");
    // let y = x;
    
    //println!("{} {}", x, y);

    // let x;
    // match get_an_optional_value() {
    //     Some(v) => x = v, // if Some("abc"), set x to "abc"
    //     None => panic!(), // if None, panic without any message
    // }

    // println!("x: {}", x); // "abc" ; if you change line 14 `false` to `true`

    // let n: Option<i8> = None;
    // n.expect("empty value returned");
    
    // let x = get_an_optional_value().unwrap();
    // println!("{}", x);
    
    // let key = "PATH";
    // match env::var(key) {
    //     Ok(v) => println!("env v: {}", v),
    //     Err(e) => println!("env e: {}", e),
    // }
    
    // match function_with_error(0) {
    //     Ok(v) => println!("v: {}", v),
    //     Err(e) => println!("e: {}", e),
    // }
    
    // match foo(-3) {
    //     None => println!("really bad!"),
    //     Some(i) => println!("ok, you got {}", i),
    // }
    
    // let a = Some(5);
    // let b = Some("hello");
    // let c: Option<i32> = None;
    
    // println!("asdf");
    
    // let x = 5;
    // let y: i32 = 6;
    // let (a, b) = (3, 4);
    // println!("Hello World, www.linuxidc.com! {} {} {} {}", x, y, a, b);

    // let h = 7;
    // let i = incr(h);
    // println!("i={}", i);

    // let mut a = String::from("hello");
    // let x = &mut a;
    // x.push('A');
    // println!("{}", x);
    //a.push('A');

    // let mut rng = thread_rng();
    // let x: u32 = rng.gen();
    // println!("{}", x);
    
    // let x = String::from("x");
    // let z;
    // let y = String::from("y");

    // z = foo(&x, &y);

    // println!("{}", z);
}

// https://stackoverflow.com/questions/31029095/disable-registry-update-in-cargo
// git clone --bare https://github.com/rust-lang/crates.io-index.git
// [registry]
// index = "file:///F:/Data/Repositories/crates.io-index.git"
