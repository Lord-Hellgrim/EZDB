package ezdb_client


import "core:fmt"
import "core:net"
import "core:crypto/x25519"
import "core:crypto"


make_connection :: proc(address: [4]u8, port: int) -> ([32]u8, net.Network_Error) {

    shared_secret : [32]u8

    net_address : net.IP4_Address = {address[0], address[1], address[2], address[3]}
    socket, socket_error := net.dial_tcp_from_address_and_port(net_address, port)
    if socket_error != nil {
        fmt.println("Failed to create socket")
        return shared_secret, socket_error
    }

    self_private_key : [32]u8
    crypto.rand_bytes(self_private_key[:])
    self_public_key : [32]u8;
    x25519.scalarmult_basepoint(self_public_key[:], self_private_key[:])

    server_public_key : [32]u8
    nr_of_bytes_read, recv_error := net.recv_tcp(socket, server_public_key[:])
    if nr_of_bytes_read != 32 || recv_error == nil {
        fmt.println("error reading server public key")
    }
    net.send_tcp(socket, self_public_key[:])
    fmt.println(server_public_key)

    x25519.scalarmult(shared_secret[:], self_private_key[:], server_public_key[:])

    return shared_secret, nil

}


main :: proc() {

    localhost :[4]u8 = {127,0,0,1}
    
    shared_secret, dh_error := make_connection(localhost, 3004)
    if dh_error != nil {
        return
    }



    fmt.println(shared_secret)

// scalarmult_basepoint with a random 32-byte scalar writes a 32-byte public key to dst
// (said random 32-byte scalar is the private key)
// scalarmut(shared_sekrit, your_private_key, their_public_key)
// basepoint is the canonical generator of the group
// in theory this only needs scalarmult since scalarmult(your_public, your_private, basepoint) is equivalent




}


EzTable :: struct {

}

Response :: struct {
    response_code: int,
    data: EzTable,
}

EzError :: enum {
    no_server,
    crypto,
}

query_table :: proc(address: string, username: string, password: string, query: string) {

}

// /// Send an EZQL query to the database server
// query_table :: proc(address: [4]u8, username: string, password: string, query: string) -> (Response, EzError) {
    
//     connection := Connection::connect(address, username, password)?;

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