from mrjob.job import MRJob

class RetailTab(MRJob):
    def mapper(self, _, line):
        if "InvoiceNo" in line:  # Skip header line
            return
        fields = line.strip().split('\t')
        invoice_no = fields[0]
        amount = int(fields[3])
        cost = float(fields[5])
        total_cost = amount * cost
        yield invoice_no, (amount, total_cost)

    def reducer(self, invoice_no, values):
        total_amount = 0
        total_cost = 0
        for amount, cost in values:
            total_amount += amount
            total_cost += cost
        yield invoice_no, (total_amount, total_cost)

if __name__ == '__main__':
    RetailTab.run()


