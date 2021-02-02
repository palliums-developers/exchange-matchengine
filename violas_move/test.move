script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main(account: &signer, a: u64, userdata: vector<u8>) {
    ViolasBank::publish2(account, a, userdata)
}
}
