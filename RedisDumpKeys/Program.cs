using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using StackExchange.Redis;

namespace RedisDumpKeys
{
    class Program
    {
        private static ConnectionMultiplexer _multiplexer;

        private static Dictionary<string, string> argDict = new Dictionary<string, string>
        {
            {"-h", "127.0.0.1"}, {"-p", "6379"}, {"-key", ""}, {"-folder", Directory.GetCurrentDirectory()},
            {"-d", "0"}
        };

        static void Main(string[] args)
        {
            string command;
            List<string> parts;
            if (args.Length <= 1)
            {
                Console.WriteLine("Привет! Я могу делать дампы ключей и потом восстанавливать их.");
                Console.WriteLine(
                    "Сделать дампы ключей: dump -h [Адрес Redis сервера] -p [порт] -d [номер БД] -key [Шаблон ключа] -folder [Директория для сохранения дампов]");
                Console.WriteLine(
                    "Развернуть дампы ключей: restore -h [Адрес Redis сервера] -p [порт] -d [номер БД] -key [Шаблон ключа] -folder [Директория с дампами]");
                command = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(command))
                {
                    throw new ArgumentException("Требуется указать команду");
                }
                parts = command.Split(" ", StringSplitOptions.RemoveEmptyEntries).ToList();
            }
            else
            {
                parts = args.ToList();
            }

            var listArgs = argDict.Keys.ToList();
            foreach (var arg in listArgs)
            {
                var index = parts.IndexOf(arg);
                if (index > -1)
                {
                    argDict[arg] = parts[index + 1].Trim();
                }
            }

            var db = int.Parse(argDict["-d"]);
            var conf = new ConfigurationOptions
            {
                DefaultDatabase = db,
                ConnectRetry = 3,
                AllowAdmin = true
            };
            conf.EndPoints.Add($"{argDict["-h"]}:{argDict["-p"]}");
            _multiplexer = ConnectionMultiplexer.Connect(conf);

            var commandType = parts.First().Trim();
            switch (commandType)
            {
                case "dump":
                {
                    Dump(_multiplexer.GetDatabase(db), argDict["-key"]);
                    Console.WriteLine("Дамп выполнен!");
                    break;
                }
                case "restore":
                {
                    Restore(_multiplexer.GetDatabase(db), argDict["-key"]);
                    Console.WriteLine("Восстановление выполнено!");
                    break;
                }
                default: throw new IndexOutOfRangeException($"Команда {commandType} не поддерживается!");
            }
        }

        private static void Dump(IDatabase database, string key)
        {
            var server = _multiplexer.GetEndPoints().Single();
            var keys = _multiplexer.GetServer(server).Keys(database.Database, $"{key}").ToList();
            Console.WriteLine($"Найдено {keys.Count} ключей");
            foreach (var redisKey in keys)
            {
                var pathToDump = Path.Combine(argDict["-folder"], redisKey.ToString().Replace(":", "@@@"));
                var dump = new ReadOnlySpan<byte>(database.KeyDump(redisKey));
                using var file = File.Create($"{pathToDump}.rrdf");
                file.Write(dump);
                file.Flush();
            }
        }

        private static void Restore(IDatabase database, string key)
        {
            var dumps = Directory.GetFiles(argDict["-folder"], $"{key.Replace(":", "@@@")}.rrdf").ToList();
            Console.WriteLine($"Найдено {dumps.Count} дампов ключей");
            foreach (var dump in dumps)
            {
                var bytes = File.ReadAllBytes(dump);
                var keyName = Path.GetFileNameWithoutExtension(dump).Replace("@@@", ":");
                database.KeyRestore(keyName, bytes);
            }
        }
    }
}