
# emysql - Erlang MySQL Driver

For I cannot find a mature, stable and well-designed mysql driver, for example [Emysql](https://github.com/Eonblast/Emysql/) project will not be maintained, I spent one week rewriting the old driver used in my opengoss product years ago.

After I finished the coding of this library, I found there has been an awesome mysql driver available: [mysql-otp](https://github.com/mysql-otp/mysql-otp). The design of mysql-otp driver is elegant and OTP-compatible, it follows the principles of driver design that socket, protocol and pool should be separated.

Finally, I decide to deprecate my emysql driver and contribute to [mysql-otp](https://github.com/mysql-otp/mysql-otp) project.


## MySQL Client Protocol

* http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

## Author

Feng Lee <feng@emqtt.io>
