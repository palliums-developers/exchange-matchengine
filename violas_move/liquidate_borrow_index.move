script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main(tokenidx: u64, borrower: address, amount: u64, collateral_tokenidx: u64, data: vector<u8>) {
    ViolasBank::liquidate_borrow_index(tokenidx, borrower, amount, collateral_tokenidx, data);
}
}

