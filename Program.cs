using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Mailjet.Client;
using Mailjet.Client.Resources;
using Newtonsoft.Json.Linq;

namespace sheets
{
    class Program
    {

        static string ConnectionString = "Data Source=LAPTOP-GSLU9DH1\\SQLEXPRESS;Initial Catalog=ECHotel;Integrated Security=True";

        static void Main(string[] args)
        {

            // InitializeApi();
            SetupMailJet().Wait();
            // var people = LoadPeopleFromDatabase();
            // int contador_contatos = 1;
            // int list_atual = CreateList(1).Wait();
            // int contador_contatos = 1;
            // long? lista_atual = 4;
            // for (var i = 0; i < people.Rows.Count; i++)
            // {
            //     DataRow person = people.Rows[i];
            //     string email = person["Email"].ToString();
            //     if (!string.IsNullOrWhiteSpace(email) && validEmail(email))
            //     {
            //         if (contador_contatos <= 300)
            //         {
            //             createContact(lista_atual, person);
            //             if (contador_contatos == 300)
            //             {
            //                 lista_atual = lista_atual += 1;
            //                 contador_contatos = 0;
            //             }
            //         }

            //         contador_contatos += 1;
            //     }
            // }
            // createList(1.ToString());

            // DataTable table = LoadDatatable();
            // foreach (DataRow row in table.Rows)
            // {
            //     // var personId = InsertPerson(row);
            //     // if (personId > 0)
            //     // {
            //     //     InsertPhones(row, personId);
            //     //     InsertEmail(row, personId);
            //     //     InsertAddress(row, personId);
            //     // }
            // }

            // for (var i = 1; i < 101; i++)
            // {
            //     SplitIntoOtherFiles(i);
            // }

            // var people = LoadPeopleFromDatabase();
            // toCsv(people, $"emailList.csv");

            // int total = 29963;
            // int iterator = (total / 200);
            // int perFile = total / iterator;

            // for (int i = 0; i < iterator; i++)
            // {
            //     var people = LoadPeopleFromDatabase(200, (200 * i));
            //     toCsv(people, $"emailList_{i}.csv");
            // }


            // CreateList(1).Wait();

            Console.WriteLine("Fim");
        }


        static async Task SetupMailJet()
        {
            MailjetClient client = new MailjetClient("260bea83d763f588100163bf77d57bdf", "3668e4ac2453ced77bbce745a48a9154");


            DataTable people = LoadDatatable();


            int contador_de_contatos = 1;
            int list_number = 1;
            int lista_inicial = await CreteMailJetList(list_number, client);

            for (int i = 0; i < people.Rows.Count; i++)
            {
                DataRow person = people.Rows[i];
                string email = person["Email"].ToString();
                // string first_name = person["FirstName"].ToString();
                // string last_name = person["LastName"].ToString();
                if (validEmail(email))// && !string.IsNullOrEmpty(first_name) && !string.IsNullOrEmpty(last_name))
                {
                    if (contador_de_contatos <= 200)
                    {
                        await AddContactToList(client, lista_inicial, person);
                        // saveSend(person["Id"].ToString());
                        if (contador_de_contatos == 200)
                        {
                            list_number += 1;
                            lista_inicial = await CreteMailJetList(list_number, client);
                            contador_de_contatos = 0;
                        }
                    }
                    contador_de_contatos += 1;
                }
            }




        }


        static async Task<int> CreteMailJetList(int number, MailjetClient client)
        {
            MailjetRequest request = new MailjetRequest
            {
                Resource = Contactslist.Resource
            }.Property(Contactslist.Name, $"SPA {number}");

            MailjetResponse response = await client.PostAsync(request);
            if (response.IsSuccessStatusCode)
            {
                var id = response.GetData().Select(x => x.Value<int>("ID")).FirstOrDefault();
                return id;
            }

            return 0;
        }


        static async Task CreateMailJetContact(int listid, MailjetClient client, DataRow row)
        {
            MailjetRequest request = new MailjetRequest
            {
                Resource = Contact.Resource
            }.Property(Contact.Name, $"{row["FirstName"].ToString()} {row["LastName"].ToString()}")
            .Property(Contact.Email, row["Email"].ToString());

            MailjetResponse response = await client.PostAsync(request);
            if (response.IsSuccessStatusCode)
            {
                var id = response.GetData().Select(x => x.Value<int>("ID")).FirstOrDefault();
                MailjetRequest request1 = new MailjetRequest
                {
                    Resource = Contactdata.Resource,
                    ResourceId = ResourceId.Numeric(id),
                }.Property(Contactdata.Data, new JArray
                {
                    new JObject
                    {
                        {"firstname", row["FirstName"].ToString()},
                        {"lastname", row["LastName"].ToString()}
                    }
                });

                await client.PutAsync(request);
            }

        }



        static async Task AddContactToList(MailjetClient client, int listId, DataRow person)
        {
            MailjetRequest req = new MailjetRequest
            {
                Resource = ContactslistManagecontact.Resource,
                ResourceId = ResourceId.Numeric(listId)
            }.Property(ContactslistManagecontact.Action, "addnoforce")
            //.Property(Contact.Name, $"{person["FirstName"].ToString()} {person["LastName"].ToString()}")
            .Property(Contact.Email, person["Email"].ToString());

            await client.PostAsync(req);


        }


        static void saveSend(string id)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                string sql = $"INSERT INTO dbo.Sents(PersonId) VALUES('{id}')";
                var command = new SqlCommand(sql, connection);
                command.ExecuteScalar();
                connection.Close();
            }
        }




        static DataTable LoadDatatable()
        {
            DataTable dt = new DataTable();
            using (var csvReader = new StreamReader(@"C:\Users\emmac\Downloads\E-mail spa - geral sistema.csv"))
            {
                string[] headers = csvReader.ReadLine().Split(';');
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }

                while (!csvReader.EndOfStream)
                {
                    string[] rows = Regex.Split(csvReader.ReadLine(), ";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }
            }

            return dt;
        }


        static DataTable LoadPeopleFromDatabase(int fetch = 0, int skip = 0)
        {
            DataTable people = new DataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                string sql = @"	SELECT DISTINCT(pl.Id) AS Id, pl.FirstName, pl.LastName, plem.Email  FROM People pl LEFT JOIN PeopleEmails plem ON plem.PersonId = pl.Id WHERE pl.Id < 29970 AND pl.Id NOT IN ((SELECT PersonId FROM dbo.Sents)) ORDER BY pl.Id ";

                if (fetch > 0)
                {
                    sql += $" offset {skip} rows FETCH NEXT {fetch} rows only ";
                }

                var command = new SqlCommand(sql, connection);
                people.Load(command.ExecuteReader());
                connection.Close();
            }
            return people;
        }

        static int InsertPerson(DataRow row)
        {
            string row_nome = row.Field<string>("nome");
            // var split = row_nome.Split(new char[] { ' ' }, 2);
            string sobrenome = row.Field<string>("sobrenome");

            if (string.IsNullOrWhiteSpace(OnlyNumberAndString(sobrenome)) || OnlyNumberAndString(sobrenome, false) == "NULL")
            {
                var split = row_nome.Split(new char[] { ' ' }, 2);
                row_nome = split[0];
                if (split.Length > 1)
                {
                    sobrenome = split[1];
                }
            }
            // if (split.Length > 1)
            // {
            //     sobrenome = split[1].ToString();
            // }

            if (OnlyNumbers(row.Field<string>("cpf")) != null)
            {
                string sql = string.Format("INSERT INTO dbo.People(Document, FirstName, LastName) VALUES('{0}', '{1}', '{2}'); SELECT SCOPE_IDENTITY()", OnlyNumbers(row.Field<string>("cpf")), OnlyNumberAndString(row_nome.ToString(), false), OnlyNumberAndString(sobrenome, false));

                object insertPerson;

                using (var sqlConnection = new SqlConnection(ConnectionString))
                {
                    sqlConnection.Open();
                    var command = new SqlCommand(sql, sqlConnection);
                    insertPerson = command.ExecuteScalar();
                    sqlConnection.Close();
                }

                return Convert.ToInt32(insertPerson);
            }
            return 0;

        }


        static void InsertPhones(DataRow row, int personId)
        {
            string sql = "INSERT INTO dbo.PeoplePhoneNumbers(PhoneNumber, PersonId) VALUES ";

            // string telefone = row.Field<string>("telefone");
            // if (!string.IsNullOrEmpty(telefone) && telefone.Length < 1)
            // {
            //     telefone = row.Field<string>("ddd") + telefone;
            // }
            string celular = row.Field<string>("celular");
            if (!string.IsNullOrEmpty(celular) && celular.Length < 1)
            {
                // celular = row.Field<string>("ddd") + celular;
            }

            List<string> telefones = new List<string>();
            // telefones.Add(telefone);
            telefones.Add(celular);

            string sql_inserts = string.Empty;

            for (var i = 0; i < telefones.Count; i++)
            {
                if (!string.IsNullOrWhiteSpace(OnlyNumbers(telefones[i])))
                {
                    sql_inserts += string.Format("('{0}', '{1}')", OnlyNumbers(telefones[i]), personId);
                }
            }

            if (!string.IsNullOrEmpty(sql_inserts))
            {
                sql_inserts = sql_inserts.Replace(")(", "),(");
                sql += sql_inserts;

                using (var sqlConnection = new SqlConnection(ConnectionString))
                {
                    sqlConnection.Open();
                    var command = new SqlCommand(sql, sqlConnection);
                    command.ExecuteScalar();
                    sqlConnection.Close();
                }
            }

        }



        static void InsertEmail(DataRow row, int personId)
        {
            string sql = string.Format("INSERT INTO dbo.PeopleEmails(Email, PersonId) VALUES('{0}', '{1}')", row.Field<string>("email"), personId);

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                var command = new SqlCommand(sql, sqlConnection);
                command.ExecuteScalar();
                sqlConnection.Close();
            }
        }


        static void InsertAddress(DataRow row, int personId)
        {

            if (!string.IsNullOrWhiteSpace(OnlyNumbers(row.Field<string>("cep"))) && (!string.IsNullOrWhiteSpace(row.Field<string>("uf")) || row.Field<string>("uf") == "NULL") && (!string.IsNullOrWhiteSpace(row.Field<string>("cidade")) || row.Field<string>("cidade") == "NULL"))
            {
                string sql = string.Format("INSERT INTO dbo.Addresses(Uf, Country, City, Zipcode, Line1, Line2, Line3) VALUES('{0}', '{1}', '{2}', '{3}', '{4}','{5}','{6}');SELECT SCOPE_IDENTITY();", OnlyNumberAndString(row.Field<string>("uf"), false), "BRA", OnlyNumberAndString(row.Field<string>("cidade"), false), OnlyNumbers(row.Field<string>("cep")), OnlyNumberAndString(row.Field<string>("endereco"), true), OnlyNumberAndString(row.Field<string>("bairro"), true), OnlyNumberAndString(row.Field<string>("complemento"), true));

                object address = null;

                using (var sqlConnection = new SqlConnection(ConnectionString))
                {
                    sqlConnection.Open();
                    var command = new SqlCommand(sql, sqlConnection);
                    address = command.ExecuteScalar();
                    sqlConnection.Close();
                }

                sql = string.Format("INSERT INTO dbo.PeopleAddresses(PeopleId, AddressId) VALUES('{0}', '{1}')", personId, Convert.ToInt32(address));

                using (var sqlConnection = new SqlConnection(ConnectionString))
                {
                    sqlConnection.Open();
                    var command = new SqlCommand(sql, sqlConnection);
                    command.ExecuteScalar();
                    sqlConnection.Close();
                }
            }

        }

        static string OnlyNumberAndString(string text, bool numbers = false)
        {
            if (numbers) return Regex.Replace(text, @"[^A-Za-z0-9 ]+", string.Empty);
            else return Regex.Replace(text, @"[^A-Za-z ]+", string.Empty);
        }

        static string OnlyNumbers(string text)
        {
            return Regex.Replace(text, @"[^0-9 ]+", string.Empty);
        }

        static bool validEmail(string email)
        {
            try
            {
                var addr = new MailAddress(email);
                return addr.Address == email;
            }
            catch
            {
                return false;
            }
        }



        static void toCsv(DataTable table, string filePath)
        {
            StreamWriter sw = new StreamWriter(filePath, false);
            for (int i = 0; i < table.Columns.Count; i++)
            {
                string columnValue = table.Columns[i].ToString();
                switch (columnValue)
                {
                    case "FirstName":
                        columnValue = "firstname";
                        break;
                    case "LastName":
                        columnValue = "lastname";
                        break;
                    case "Email":
                        columnValue = "email";
                        break;
                    default:
                        break;
                }
                sw.Write(columnValue);
                if (i < table.Columns.Count - 1)
                {
                    sw.Write(",");
                }
            }

            sw.Write(sw.NewLine);
            foreach (DataRow dr in table.Rows)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    if (!Convert.IsDBNull(dr[i]))
                    {
                        string value = dr[i].ToString();
                        if (value.Contains(','))
                        {
                            value = String.Format("\"{0}\"", value);
                            sw.Write(value);
                        }
                        else
                        {
                            sw.Write(value);
                        }
                    }

                    if (i < table.Columns.Count - 1)
                    {
                        sw.Write(",");
                    }
                }
                sw.Write(sw.NewLine);
            }
            sw.Close();
        }
    
        
    
    }
}
