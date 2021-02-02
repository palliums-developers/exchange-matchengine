script {
use 0x1::ViolasBank;

fun main(account: &signer, tokenidx: u64, amount: u64, data: vector<u8>) {
    ViolasBank::borrow_index(account, 0x1::Vector::empty(), tokenidx, amount, data);
}
}
