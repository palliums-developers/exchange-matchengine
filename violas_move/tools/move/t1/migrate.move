script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main(account: &signer) {
    ViolasBank::migrate_data(account)
}
}
