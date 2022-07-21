using System.Threading.Tasks;
using dotnet_etcd;
using DotnetNiceGrpcLogs;
using Etcdserverpb;
using NUnit.Framework;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace Integration;

public class DevelopmentProcessTests
{
    

    [SetUp]
    public async Task Setup()
    {
        await Framework.CleanEtcdTestsKeys();
    }

    [TearDown]
    public async Task TearDownAsync()
    {
        await Framework.CleanEtcdTestsKeys();
    }

    //debug space here
    [Test]
    public async Task Test1()
    {
    }
}