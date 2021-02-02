script {
use 0x1::ViolasBank;

fun main(account: &signer) {
    if(ViolasBank::is_published(account) == false) {
	ViolasBank::publish(account, x"00");
    };
    ViolasBank::migrate_data(account)
}
}
