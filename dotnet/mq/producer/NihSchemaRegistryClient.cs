namespace producer;

using Confluent.SchemaRegistry;

public class NihSchemaRegistryClient: ISchemaRegistryClient
{
    private readonly ISchemaRegistryClient _innerClient;
    public NihSchemaRegistryClient(ISchemaRegistryClient innerClient, int maxCachedSchemas = 500)
    {
        _innerClient = innerClient;
        MaxCachedSchemas = maxCachedSchemas;
    }
    public void Dispose()
    {
        _innerClient.Dispose();
    }

    public Task<int> RegisterSchemaAsync(string subject, string schema) => _innerClient.RegisterSchemaAsync(subject, schema);

    public Task<int> GetSchemaIdAsync(string subject, string schema) => _innerClient.GetSchemaIdAsync(subject, schema);

    public Task<string> GetSchemaAsync(int id) => _innerClient.GetSchemaAsync(id);

    public Task<string> GetSchemaAsync(string subject, int version) => _innerClient.GetSchemaAsync(subject, version);

    public Task<Schema> GetLatestSchemaAsync(string subject) => _innerClient.GetLatestSchemaAsync(subject);

    public Task<List<string>> GetAllSubjectsAsync() => _innerClient.GetAllSubjectsAsync();

    public Task<List<int>> GetSubjectVersionsAsync(string subject) => _innerClient.GetSubjectVersionsAsync(subject);

    public Task<bool> IsCompatibleAsync(string subject, string schema) => _innerClient.IsCompatibleAsync(subject, schema);

    public string ConstructKeySubjectName(string topic, string recordType = null!) => $"{topic}.{recordType}.key";

    public string ConstructValueSubjectName(string topic, string recordType = null!) => $"{topic}.{recordType}.value";

    public int MaxCachedSchemas { get; }
}