script {
use 0x1::ViolasBank;

fun main(account: &signer) {
    ViolasBank::migrate_data(account)
}
}
