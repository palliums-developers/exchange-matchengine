script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main(tokenidx: u64, payee: address, amount: u64, data: vector<u8>) {
    ViolasBank::mint(tokenidx, payee, amount, data);
}
}
