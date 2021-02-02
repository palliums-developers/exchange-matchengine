script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank2;

fun main(account: &signer) {
    ViolasBank2::migrate_data(account)
}
}
