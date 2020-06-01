script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main(tokenidx: u64, amount: u64, data: vector<u8>) {
    ViolasBank::lock_index(tokenidx, amount, data);
}
}
