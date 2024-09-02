package ezdb_client


import "core:fmt"
import "core:net"
import "core:crypto/x25519"

main :: proc() {

    localhost : net.IP4_Address = {127,0,0,1}
    socket, socket_error := net.dial_tcp_from_address_and_port(localhost, 3004)
    if socket_error != nil {
        fmt.println("Failed to create socket")
        return
    }

    self_private_key := crypto.x25519.

    buffer : [32]u8
    nr_of_bytes_read, recv_error := net.recv_tcp(socket, buffer[:])
    if nr_of_bytes_read != 32 || recv_error == nil {
        fmt.println("error reading server public key")
    }
    fmt.println(buffer)




}


EzTable :: struct {

}

Response :: struct {
    response_code: int,
    data: EzTable,
}

query_table :: proc(address: string, username: string, password: string, query: string) {

}

// /// Send an EZQL query to the database server
// pub fn query_table(
//     address: &str,
//     username: &str,
//     password: &str,
//     query: &str,
// ) -> Result<Response, EzError> {
//     let mut connection = Connection::connect(address, username, password)?;

//     let response = instruction_send_and_confirm(
//         Instruction::Query(query.to_owned()),
//         &mut connection,
//     )?;
//     println!("HERE 1!!!");
//     let data: Vec<u8>;
//     match response.as_str() {
        
//         // THIS IS WHERE YOU SEND THE BULK OF THE DATA
//         //########## SUCCESS BRANCH #################################
//         "OK" => data = receive_data(&mut connection)?,
//         //###########################################################
//         "Username is incorrect" => {
//             return Err(EzError::Authentication(AuthenticationError::WrongUser(
//                 connection.user,
//             )))
//         }
//         "Password is incorrect" => {
//             return Err(EzError::Authentication(
//                 AuthenticationError::WrongPassword,
//             ))
//         }
//         e => panic!("Need to handle error: {}", e),
//     };
//     println!("HERE 2!!!");
//     println!("received data:\n{}", bytes_to_str(&data)?);

//     match connection.stream.write("OK".as_bytes()) {
//         Ok(n) => println!("Wrote 'OK' as {n} bytes"),
//         Err(e) => {
//             return Err(EzError::Io(e.kind()));
//         }
//     };

//     match String::from_utf8(data.clone()) {
//         Ok(x) => Ok(Response::Message(x)),
//         Err(_) => match ColumnTable::from_binary("RESULT", &data) {
//             Ok(table) => Ok(Response::Table(table)),
//             Err(e) => Err(e.into()),
//         },
//     }
// }