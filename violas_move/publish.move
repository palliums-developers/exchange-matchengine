script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main(userdata: vector<u8>) {
    ViolasBank::publish(userdata)
}
}
