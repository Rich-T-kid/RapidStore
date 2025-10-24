package memorystore

import (
	"encoding/json"
	"time"

	"RapidStore/memoryStore/internal/DS"
)

type AdaptableState interface {
	Serialize() ([]byte, error) // take internal memory state and convert to byte slice
	Deserialize([]byte) error   // take byte slice and convert back to internal memory state
}

// Mock struct for testing the AdaptableState interface
type MockState struct {
	ID       int               `json:"id"`
	Name     string            `json:"name"`
	Values   []int             `json:"values"`
	Metadata map[string]string `json:"metadata"`
}

func NewMockState() AdaptableState {
	return &MockState{
		ID:       0,
		Name:     "",
		Values:   make([]int, 0),
		Metadata: make(map[string]string),
	}
}

func (m *MockState) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MockState) Deserialize(data []byte) error {
	return json.Unmarshal(data, m)
}

func NewAdaptableState() AdaptableState {
	return &RapidStoreServer{}
}

// outputs internal memory state as byte slice
func (r *RapidStoreServer) dumpState() ([]byte, error) {
	state := map[string]interface{}{}

	// Serialize KeyManager data
	if keyStore, ok := r.KeyManger.(*keyStore); ok {
		state["keys"] = keyStore.internalData
	}

	// Serialize HashManager data
	if fieldStore, ok := r.HashManager.(*FieldStore); ok {
		state["hashes"] = fieldStore.FieldData
	}

	// Serialize SequenceManager data
	if orderedListStore, ok := r.SequenceManager.(*OrderedListStore); ok {
		// Convert SequenceStorage to serializable format
		sequenceData := make(map[string][]interface{})
		for key, sequence := range orderedListStore.internalManager {
			// Use Range to get all values from the linked list
			size := int(sequence.Size())
			if size > 0 {
				values := sequence.Range(0, size-1)
				// Extract actual values from Node pointers
				var nodeValues []interface{}
				for _, nodeInterface := range values {
					if node, ok := nodeInterface.(*DS.Node); ok {
						nodeValues = append(nodeValues, node.Value)
					}
				}
				sequenceData[key] = nodeValues
			} else {
				sequenceData[key] = []interface{}{}
			}
		}
		state["sequences"] = sequenceData
	}

	// Serialize SetManager data
	if uniqueSetStore, ok := r.SetM.(*UniqueSetStore); ok {
		// Convert set structure to serializable format
		setData := make(map[string][]interface{})
		for key, set := range uniqueSetStore.internalManager {
			var members []interface{}
			for member := range set {
				members = append(members, member)
			}
			setData[key] = members
		}
		state["sets"] = setData
	}

	// Serialize SortedSetManager data
	if sortedSetStore, ok := r.SortSetM.(*SortedSetStore); ok {
		// Convert sorted set structure to serializable format
		sortedSetData := make(map[string]map[string]interface{})
		for key, tree := range sortedSetStore.internalStore {
			// Get all key-value pairs from the BST using InOrder traversal
			nodes := tree.InOrder()
			memberScores := make(map[string]interface{})
			for _, node := range nodes {
				if member, ok := node.Value.(string); ok {
					memberScores[member] = node.Key
				}
			}
			sortedSetData[key] = memberScores
		}
		state["sortedSets"] = sortedSetData
	}

	return json.Marshal(state)
}

// loads internal memory state from byte slice
func (r *RapidStoreServer) loadState(data []byte) error {
	var state map[string]interface{}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	// Deserialize KeyManager data
	if keysData, exists := state["keys"]; exists && r.KeyManger != nil {
		if keyStore, ok := r.KeyManger.(*keyStore); ok {
			if keysMap, ok := keysData.(map[string]interface{}); ok {
				for key, valueInterface := range keysMap {
					if valueMap, ok := valueInterface.(map[string]interface{}); ok {
						// Reconstruct GeneralValue from JSON
						var gv GeneralValue
						if value, exists := valueMap["Value"]; exists {
							gv.Value = value
						}
						if ttlStr, exists := valueMap["TTL"]; exists {
							if ttlString, ok := ttlStr.(string); ok {
								if ttl, err := time.Parse(time.RFC3339, ttlString); err == nil {
									gv.TTL = ttl
								}
							}
						}
						keyStore.internalData[key] = gv
					}
				}
			}
		}
	}

	// Deserialize HashManager data
	if hashesData, exists := state["hashes"]; exists && r.HashManager != nil {
		if fieldStore, ok := r.HashManager.(*FieldStore); ok {
			if hashesMap, ok := hashesData.(map[string]interface{}); ok {
				for key, fieldsInterface := range hashesMap {
					if fieldsMap, ok := fieldsInterface.(map[string]interface{}); ok {
						fieldData := make(map[string]GeneralValue)
						for field, valueInterface := range fieldsMap {
							if valueMap, ok := valueInterface.(map[string]interface{}); ok {
								var gv GeneralValue
								if value, exists := valueMap["Value"]; exists {
									gv.Value = value
								}
								if ttlStr, exists := valueMap["TTL"]; exists {
									if ttlString, ok := ttlStr.(string); ok {
										if ttl, err := time.Parse(time.RFC3339, ttlString); err == nil {
											gv.TTL = ttl
										}
									}
								}
								fieldData[field] = gv
							}
						}
						fieldStore.FieldData[key] = fieldData
					}
				}
			}
		}
	}

	// Deserialize SequenceManager data
	if sequencesData, exists := state["sequences"]; exists && r.SequenceManager != nil {
		if orderedListStore, ok := r.SequenceManager.(*OrderedListStore); ok {
			if sequencesMap, ok := sequencesData.(map[string]interface{}); ok {
				for key, valuesInterface := range sequencesMap {
					if valuesList, ok := valuesInterface.([]interface{}); ok {
						// Create new sequence storage and populate it
						sequence := DS.NewSequenceStorage()
						for _, value := range valuesList {
							sequence.AddLast(value)
						}
						orderedListStore.internalManager[key] = sequence
					}
				}
			}
		}
	}

	// Deserialize SetManager data
	if setsData, exists := state["sets"]; exists && r.SetM != nil {
		if uniqueSetStore, ok := r.SetM.(*UniqueSetStore); ok {
			if setsMap, ok := setsData.(map[string]interface{}); ok {
				for key, membersInterface := range setsMap {
					if membersList, ok := membersInterface.([]interface{}); ok {
						setData := make(map[interface{}]struct{})
						for _, member := range membersList {
							setData[member] = struct{}{}
						}
						uniqueSetStore.internalManager[key] = setData
					}
				}
			}
		}
	}

	// Deserialize SortedSetManager data
	if sortedSetsData, exists := state["sortedSets"]; exists && r.SortSetM != nil {
		if sortedSetStore, ok := r.SortSetM.(*SortedSetStore); ok {
			if sortedSetsMap, ok := sortedSetsData.(map[string]interface{}); ok {
				for key, membersInterface := range sortedSetsMap {
					if membersMap, ok := membersInterface.(map[string]interface{}); ok {
						// Create new binary tree
						tree := DS.NewBinaryTree[float64]()
						for member, scoreInterface := range membersMap {
							if score, ok := scoreInterface.(float64); ok {
								tree.Insert(score, member)
								sortedSetStore.mbtoval[member] = score
							}
						}
						sortedSetStore.internalStore[key] = tree
					}
				}
			}
		}
	}

	return nil
}

func (r *RapidStoreServer) Serialize() ([]byte, error)    { return r.dumpState() }
func (r *RapidStoreServer) Deserialize(data []byte) error { return r.loadState(data) }
