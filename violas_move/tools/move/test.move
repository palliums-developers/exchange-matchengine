script {
use 0x1::Signer;
//use 0x1::ViolasBank;

fun main<Token>(account: &signer) {
    let _sender = Signer::address_of(account);
    //ViolasBank::print_balance<Token>(sender);
}
}
