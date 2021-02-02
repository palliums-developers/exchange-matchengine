script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main(account: &signer, tokenidx: u64, payee: address, amount: u64, data: vector<u8>) {
    ViolasBank2::mint(account, tokenidx, payee, amount, data);
}
}
