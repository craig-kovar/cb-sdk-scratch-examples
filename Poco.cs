namespace sdk2code
{
    class Poco
    {

        string name;
        string email;
        public Poco() {}

        public Poco(string name, string email) {
            this.name = name;
            this.email = email;
        }

        public void setName(string name) {
            this.name = name;
        }

        public void setEmail(string email) {
            this.email = email;
        }

        public string getName() {
            return this.name;
        }

        public string getEmail() {
            return this.email;
        }

    }
}