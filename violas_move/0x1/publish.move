script {
use 0x1::ViolasBank;

fun main(account: &signer, userdata: vector<u8>) {
    ViolasBank::publish(account, userdata)
}
}
