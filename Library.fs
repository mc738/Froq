namespace Froq

open System
open MySql.Data.MySqlClient

module private Helpers =

    let prepareQuery (query: string) (connection: MySqlConnection) (parameters: Map<string, obj>) =
        use comm = new MySqlCommand(query, connection)

        parameters
        |> Map.map (fun k v -> comm.Parameters.AddWithValue(k, v))
        |> ignore

        comm.Prepare()

        comm

    (*
    let executeNonQueryCommand (command: MySqlCommand) = command.ExecuteNonQuery()
    *)
(*
type NonQueryHandler<'a> =
    { Query: string
      Mapper: Guid -> 'a -> Map<string, string> }


type QueryHandler<'a> =
    { Query: string
      Parameters: Map<string, string>
      ResultMapper: MySqlDataReader -> 'a }
*)
type Context =
    { Connection: MySqlConnection }

    static member Create(connStr: string) =
        use conn = new MySqlConnection(connStr)
        { Connection = conn }
        
    member context.OpenConnection() = context.Connection.Open()
    
    member context.CloseConnection() = context.Connection.Close()

type Query<'p, 'r> =
    { Sql: string
      ParameterMapper: Option<'p -> Map<string, obj>>
      ResultMapper: MySqlDataReader -> 'r }
    
    member query.Execute (context: Context, parameters: 'p) =
        context.OpenConnection()
        
        use comm =
            match query.ParameterMapper with
            | Some pm -> Helpers.prepareQuery query.Sql context.Connection (pm parameters)
            | None -> new MySqlCommand(query.Sql, context.Connection)

        use reader = comm.ExecuteReader()

        let results = query.ResultMapper reader
        context.CloseConnection()
        results

    member query.ExecuteList (context: Context, parameters: 'p list) =
        context.OpenConnection()
       
        let results =
            parameters
            |> List.map (fun p ->
                use comm =
                    match query.ParameterMapper with
                    | Some pm -> Helpers.prepareQuery query.Sql context.Connection (pm p)
                    | None -> new MySqlCommand(query.Sql, context.Connection)
                use reader = comm.ExecuteReader()
                query.ResultMapper reader)
        
        context.CloseConnection()
        results