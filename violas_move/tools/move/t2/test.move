script {
use 0x1::Signer;
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main<Token>(account: &signer) {
    let sender = Signer::address_of(account);
    ViolasBank2::print_balance<Token>(sender);
}
}
