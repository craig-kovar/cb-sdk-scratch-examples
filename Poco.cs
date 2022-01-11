using Newtonsoft.Json;

namespace sdk2to3
{
    class Poco
    {

        [JsonProperty("name")]
        public string Name {get; set; }
        
        [JsonProperty("email")]
        public string Email {get; set; }
        public Poco() {}

        public Poco(string name, string email) {
            this.Name = name;
            this.Email = email;
        }

    }
}