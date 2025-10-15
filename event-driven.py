import simpy

# --- 1. Παράμετροι Προσομοίωσης ---
# Σταθερές για να μπορούμε εύκολα να αλλάξουμε τις συνθήκες του δικτύου.

# Bandwidth σε bits ανά δευτερόλεπτο (bps). 1 Mbps = 1,000,000 bps.
BANDWIDTH_BPS = 1_000_000
# Μετατροπή του bandwidth σε bytes ανά δευτερόλεπτο, αφού τα πακέτα μετριούνται σε bytes.
BANDWIDTH_BYTES_PER_SEC = BANDWIDTH_BPS / 8

PACKET_SIZE_BYTES = 1000  # Μέγεθος κάθε πακέτου σε bytes (π.χ. 1 KB).
PACKET_INTERVAL_S = 0.01  # Χρόνος μεταξύ της αποστολής δύο διαδοχικών πακέτων (σε δευτερόλεπτα).

# Καθυστέρηση διάδοσης για κάθε σύνδεση (σε δευτερόλεπτα).
PROP_DELAY_LINK1_S = 0.050  # 50 ms για τη σύνδεση Source -> Router
PROP_DELAY_LINK2_S = 0.050  # 50 ms για τη σύνδεση Router -> Destination

SIM_TIME_S = 0.2  # Συνολικός χρόνος προσομοίωσης (σε δευτερόλεπτα).


# --- 2. Μοντέλο Πακέτου ---
# Μια απλή κλάση για να αποθηκεύουμε τις πληροφορίες κάθε πακέτου.
class Packet:
    def __init__(self, time_created, size_bytes, packet_id):
        self.time_created = time_created  # Η χρονική στιγμή δημιουργίας του πακέτου.
        self.size_bytes = size_bytes      # Το μέγεθός του σε bytes.
        self.packet_id = packet_id        # Ένας μοναδικός αριθμός για αναγνώριση.

    def __repr__(self):
        """Μια απλή αναπαράσταση του αντικειμένου για εκτύπωση."""
        return f"Packet(id={self.packet_id}, size={self.size_bytes} B)"


# --- 3. Συμπεριφορά των Κόμβων του Δικτύου ---

def source_host(env, packet_interval, packet_size, link_to_router):
    """
    Αυτή η συνάρτηση μοντελοποιεί τον υπολογιστή-πηγή.
    Δημιουργεί πακέτα σε σταθερά χρονικά διαστήματα.
    """
    packet_id_counter = 0
    while True:
        # Δημιουργία ενός νέου πακέτου.
        packet = Packet(
            time_created=env.now,
            size_bytes=packet_size,
            packet_id=packet_id_counter
        )
        print(f"Χρόνος {env.now:.4f}: [Source] Δημιούργησε και στέλνει το Πακέτο {packet.packet_id}")
        
        # "Τοποθέτηση" του πακέτου στη σύνδεση προς τον router.
        link_to_router.put(packet)
        
        # Αύξηση του μετρητή για το επόμενο πακέτο.
        packet_id_counter += 1
        
        # Αναμονή για το καθορισμένο χρονικό διάστημα πριν τη δημιουργία του επόμενου πακέτου.
        yield env.timeout(packet_interval)


def router(env, bandwidth_bytes_per_sec, prop_delay_s, link_from_source, link_to_dest):
    """
    Μοντελοποιεί τον δρομολογητή. Λαμβάνει πακέτα, προσομοιώνει τις καθυστερήσεις
    μετάδοσης και διάδοσης, και τα προωθεί.
    """
    while True:
        # Αναμονή μέχρι να λάβει ένα πακέτο από τη σύνδεση με την πηγή.
        packet = yield link_from_source.get()
        print(f"Χρόνος {env.now:.4f}: [Router] Έλαβε το Πακέτο {packet.packet_id}")

        # 1. Υπολογισμός και προσομοίωση της Καθυστέρησης Μετάδοσης (Transmission Delay).
        transmission_delay = packet.size_bytes / bandwidth_bytes_per_sec
        yield env.timeout(transmission_delay)
        print(f"Χρόνος {env.now:.4f}: [Router] Ολοκλήρωσε τη μετάδοση του Πακέτου {packet.packet_id} (delay: {transmission_delay:.4f}s)")

        # 2. Προσομοίωση της Καθυστέρησης Διάδοσης (Propagation Delay).
        yield env.timeout(prop_delay_s)
        print(f"Χρόνος {env.now:.4f}: [Router] Το Πακέτο {packet.packet_id} έφτασε στο τέλος της γραμμής προς τον προορισμό (prop delay: {prop_delay_s:.4f}s)")

        # Προώθηση του πακέτου στην επόμενη σύνδεση.
        link_to_dest.put(packet)


def destination_host(env, prop_delay_s, link_from_router):
    """
    Μοντελοποιεί τον υπολογιστή-προορισμό. Λαμβάνει πακέτα και υπολογίζει
    τη συνολική καθυστέρηση από άκρο σε άκρο (end-to-end delay).
    """
    while True:
        # Αναμονή για να λάβει ένα πακέτο από τη σύνδεση με τον router.
        packet = yield link_from_router.get()
        
        # Προσομοίωση της καθυστέρησης διάδοσης της δεύτερης σύνδεσης.
        yield env.timeout(prop_delay_s)
        
        # Υπολογισμός της συνολικής καθυστέρησης.
        end_to_end_delay = env.now - packet.time_created
        
        print(f"--------------------------------------------------------------------")
        print(f"Χρόνος {env.now:.4f}: [Destination] Έλαβε το Πακέτο {packet.packet_id}")
        print(f"    >> Συνολική Καθυστέρηση (End-to-End Delay): {end_to_end_delay:.4f} δευτερόλεπτα.")
        print(f"--------------------------------------------------------------------")


# --- 4. Κύριο Μέρος του Προγράμματος ---
if __name__ == "__main__":
    print("--- Έναρξη Προσομοίωσης Απλού Δικτύου ---")
    
    # Δημιουργία του περιβάλλοντος προσομοίωσης του SimPy.
    env = simpy.Environment()
    
    # Δημιουργία των "καναλιών" (links) που συνδέουν τους κόμβους.
    # Το simpy.Store λειτουργεί σαν ένας σωλήνας επικοινωνίας.
    link_source_to_router = simpy.Store(env)
    link_router_to_destination = simpy.Store(env)
    
    # Δημιουργία και εκκίνηση των διαδικασιών (processes) για κάθε κόμβο.
    env.process(source_host(env, PACKET_INTERVAL_S, PACKET_SIZE_BYTES, link_source_to_router))
    env.process(router(env, BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK1_S, link_source_to_router, link_router_to_destination))
    env.process(destination_host(env, PROP_DELAY_LINK2_S, link_router_to_destination))
    
    # Εκτέλεση της προσομοίωσης για καθορισμένο χρονικό διάστημα.
    env.run(until=SIM_TIME_S)
    
    print("\n--- Λήξη Προσομοίωσης ---")