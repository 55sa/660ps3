#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    // TODO pa1
    // NOTE: header and data should point to locations inside the page buffer. Do not allocate extra memory.
    capacity = DEFAULT_PAGE_SIZE * 8 / (td.length() * 8 + 1);
    header = page.data();
    data = header + DEFAULT_PAGE_SIZE - td.length() * capacity;
}

size_t HeapPage::begin() const {
    // TODO pa1
    for (size_t i = 0; i < capacity; i++) {
        if (!empty(i)) {
            return i;
        }
    }
    return capacity;
}

size_t HeapPage::end() const {
    // TODO pa1
    return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
    // TODO pa1
    size_t slot = 0;
    while (slot < capacity && (header[slot / 8] & (1 << (7 - slot % 8)))) {
        slot++;
    }
    if (slot == capacity) {
        return false;
    }
    header[slot / 8] |= 1 << (7 - slot % 8);
    uint8_t *slotData = data + slot * td.length();
    td.serialize(slotData, t);
    return true;
}

void HeapPage::deleteTuple(size_t slot) {
    // TODO pa1
    if (slot >= capacity) {
        throw std::runtime_error("Out of index");
    }
    if (empty(slot)) {
        throw std::runtime_error("Slot not occupied");
    }
    header[slot / 8] &= ~(1 << (7 - slot % 8));
}

Tuple HeapPage::getTuple(size_t slot) const {
    // TODO pa1
    if (empty(slot)) {
        throw std::runtime_error("Slot not occupied");
    }
    uint8_t *slotData = data + slot * td.length();
    return td.deserialize(slotData);
}

void HeapPage::next(size_t &slot) const {
    // TODO pa1
    while (++slot < capacity && empty(slot));
}

bool HeapPage::empty(size_t slot) const {
    // TODO pa1
    return !(header[slot / 8] & (1 << (7 - slot % 8)));
}
