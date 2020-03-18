package demo.kakfa;

public class Event {
    private String name;
    private Long price;
    private Long tax;


    public Event(EventBuilder builder) {
        name = builder.name;
        this.price = builder.price;
        this.tax = builder.tax;
    }



    public String getName() {
        return name;
    }

    public Long getPrice() {
        return price;
    }

    public Long getTax() {
        return tax;
    }

    public String toJson() {
        return new com.google.gson.Gson().toJson(this);
    }

    public static class EventBuilder {
        public String name;
        private Long price;
        private Long tax;

        public EventBuilder withName(String name) {
             this.name  =name;
             return this;
        }

        public EventBuilder withPrice(Long price) {
            this.price=price;
            return this;
        }

        public EventBuilder withTax(Long tax) {
            this.tax =tax;
            return this;
        }

        public Event build(){
            // TODO validate all mandatory props
            return new Event(this);
        }
    }
}
