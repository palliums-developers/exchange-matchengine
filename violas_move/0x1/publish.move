script {
use 0x1::ViolasBank2;

fun main(account: &signer, userdata: vector<u8>) {
    ViolasBank2::publish(account, userdata)
}
}
