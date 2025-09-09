"use client"

import { useCallback, useState, useEffect } from "react"
import { useMap, Marker, Popup, Source, Layer } from "@repo/map"
import { Box, Typography } from "@repo/ui/mui"
import { useCalSimToggle } from "./CalSimContext"
import { LocationOnIcon } from "@repo/ui/mui"

// Simplified interfaces for trail mode
export interface TrailNode {
  id: number
  short_code: string
  name: string
  coordinates: [number, number]
  element_type: string
  display_name: string
  infrastructure_type?: string
  capacity_taf?: number
  rank?: number
}

export interface TrailArc {
  id: number
  short_code: string
  name: string
  geometry: {
    type: "MultiLineString" | "LineString"
    coordinates: number[][] | number[][][]
  }
  from_node: string
  to_node: string
  display_name: string
}

const API_BASE_URL = process.env.NEXT_PUBLIC_COEQWAL_API_URL || "https://api.coeqwal.org"

function convertTrailGeoJSON(geoJsonResponse: any): { nodes: TrailNode[]; arcs: TrailArc[] } {
  const nodes: TrailNode[] = []
  const arcs: TrailArc[] = []

  geoJsonResponse.features?.forEach((feature: any) => {
    if (feature.properties.type === "node") {
      if (!feature.geometry?.coordinates) return
      
      nodes.push({
        id: feature.properties.id,
        short_code: feature.properties.short_code,
        name: feature.properties.display_name || feature.properties.short_code,
        coordinates: feature.geometry.coordinates,
        element_type: feature.properties.element_type,
        display_name: feature.properties.display_name || feature.properties.short_code,
        infrastructure_type: feature.properties.infrastructure_type,
        capacity_taf: feature.properties.capacity_taf,
        rank: feature.properties.rank,
      })
    } else if (feature.properties.type === "arc") {
      if (!feature.geometry?.coordinates) return
      
      arcs.push({
        id: feature.properties.id,
        short_code: feature.properties.short_code,
        name: feature.properties.display_name || feature.properties.short_code,
        geometry: feature.geometry,
        from_node: feature.properties.from_node || "",
        to_node: feature.properties.to_node || "",
        display_name: feature.properties.display_name || feature.properties.short_code,
      })
    }
  })

  return { nodes, arcs }
}

export default function CalSimMarkers() {
  const { isCalSimVisible } = useCalSimToggle()
  const { mapRef } = useMap()
  
  // TRAIL MODE STATE - Much simpler than full network
  const [majorReservoirs, setMajorReservoirs] = useState<TrailNode[]>([])
  const [selectedReservoir, setSelectedReservoir] = useState<TrailNode | null>(null)
  const [waterTrail, setWaterTrail] = useState<{nodes: TrailNode[], arcs: TrailArc[]}>({nodes: [], arcs: []})
  const [hoveredNode, setHoveredNode] = useState<TrailNode | null>(null)
  const [isLoadingTrail, setIsLoadingTrail] = useState(false)
  const [trailMetadata, setTrailMetadata] = useState<any>(null)

  // Load major reservoirs - hardcoded California system
  const loadMajorReservoirs = useCallback(async () => {
    console.log("🏞️ Loading major California reservoirs for TRAIL MODE...")
    
    // Hardcoded major reservoirs based on California water system
    const majorReservoirCodes = ["SHSTA", "OROVL", "FOLSM", "SLUIS", "HETCH", "TRNTY", "WKYTN", "AMADR", "MILLR"]
    
    try {
      const response = await fetch(
        `${API_BASE_URL}/api/network/elements/search?element_type=STR&limit=50`
      )
      
      if (!response.ok) {
        throw new Error(`Major reservoirs API failed: ${response.status}`)
      }
      
      const data = await response.json()
      const { nodes } = convertTrailGeoJSON(data)
      
      // Filter to major California reservoirs and add rank
      const majorNodes = nodes
        .filter(node => majorReservoirCodes.includes(node.short_code))
        .map((node, index) => ({
          ...node,
          rank: index + 1,
          system_name: _getReservoirSystemName(node.short_code)
        }))
      
      setMajorReservoirs(majorNodes)
      console.log(`✅ Loaded ${majorNodes.length} major California reservoirs for trail mode`)
      console.log(`🎯 Systems: ${majorNodes.map(n => `${n.short_code} (${n.system_name})`).join(', ')}`)
      
    } catch (error) {
      console.error("❌ Failed to load major reservoirs:", error)
      setMajorReservoirs([])
    }
  }, [])

  // Get reservoir system name for display
  const _getReservoirSystemName = (code: string): string => {
    const systems: Record<string, string> = {
      'SHSTA': 'Shasta-Sacramento',
      'OROVL': 'Oroville-Feather',
      'FOLSM': 'Folsom-American',
      'SLUIS': 'San Luis-Delta',
      'HETCH': 'Hetch Hetchy-Tuolumne',
      'TRNTY': 'Trinity Transfer',
      'WKYTN': 'Whiskeytown',
      'AMADR': 'Amador-Mokelumne',
      'MILLR': 'Millerton-San Joaquin'
    }
    return systems[code] || code
  }

  // Load water trail using new trail API
  const loadWaterTrail = useCallback(async (reservoir: TrailNode) => {
    setIsLoadingTrail(true)
    setSelectedReservoir(reservoir)
    
    console.log(`🌊 Loading WATER TRAIL from ${reservoir.short_code} using TRAIL API...`)
    
    try {
      // NEW: Use water trail API for curated "connect the dots" view
      const trailUrl = `${API_BASE_URL}/api/network/trail/${reservoir.short_code}?trail_type=infrastructure&max_depth=6`
      console.log(`📡 Fetching TRAIL API: ${trailUrl}`)
      
      const response = await fetch(trailUrl)
      if (!response.ok) {
        console.warn(`Trail API failed: ${response.status}, trying fallback...`)
        
        // FALLBACK: Use regular traversal but filter on frontend
        const fallbackUrl = `${API_BASE_URL}/api/network/traverse/${reservoir.short_code}/geopackage?direction=both&max_depth=6`
        const fallbackResponse = await fetch(fallbackUrl)
        
        if (!fallbackResponse.ok) {
          throw new Error(`Both APIs failed: Trail ${response.status}, Fallback ${fallbackResponse.status}`)
        }
        
        const fallbackData = await fallbackResponse.json()
        const networkData = convertTrailGeoJSON(fallbackData)
        
        // Filter on frontend for fallback
        const trailNodes = networkData.nodes.filter(node => 
          ['STR', 'PS', 'WTP', 'WWTP'].includes(node.element_type) ||
          (node.element_type === 'CH' && node.name.includes('River'))
        ).slice(0, 20)  // Limit to 20 for clarity
        
        const trailNodeCodes = new Set(trailNodes.map(n => n.short_code))
        const trailArcs = networkData.arcs.filter(arc => 
          trailNodeCodes.has(arc.from_node) && trailNodeCodes.has(arc.to_node)
        )
        
        setWaterTrail({ nodes: trailNodes, arcs: trailArcs })
        setTrailMetadata({ approach: 'fallback_filtered', total_features: trailNodes.length + trailArcs.length })
        
        console.log(`🌊 FALLBACK trail: ${trailNodes.length} nodes, ${trailArcs.length} arcs`)
        return
      }
      
      const trailData = await response.json()
      const { nodes, arcs } = convertTrailGeoJSON(trailData)
      
      setWaterTrail({ nodes, arcs })
      setTrailMetadata(trailData.metadata)
      
      console.log(`✅ TRAIL API loaded:`)
      console.log(`   🎯 Trail nodes: ${nodes.length} (curated for clarity)`)
      console.log(`   🛤️ Trail arcs: ${arcs.length} (connecting pathways)`)
      console.log(`   📊 Reduction: ${trailData.metadata?.reduction_ratio || 'N/A'}`)
      console.log(`   🌊 Foundation: ${trailData.metadata?.foundation || 'clean_geopackage'}`)
      
    } catch (error) {
      console.error("Failed to load water trail:", error)
      setWaterTrail({ nodes: [], arcs: [] })
      setTrailMetadata(null)
    } finally {
      setIsLoadingTrail(false)
    }
  }, [])

  // Load reservoirs when CalSim is enabled
  useEffect(() => {
    if (isCalSimVisible) {
      loadMajorReservoirs()
    } else {
      setMajorReservoirs([])
      setSelectedReservoir(null)
      setWaterTrail({ nodes: [], arcs: [] })
      setHoveredNode(null)
      setIsLoadingTrail(false)
      setTrailMetadata(null)
    }
  }, [isCalSimVisible, loadMajorReservoirs])

  // Don't render if not visible
  if (!isCalSimVisible) {
    return null
  }

  console.log(`🎨 TRAIL MODE: ${majorReservoirs.length} reservoirs + ${waterTrail.nodes.length} trail nodes + ${waterTrail.arcs.length} trail arcs`)

  return (
    <>
      {/* WATER TRAIL ARCS - Clean connecting pathways */}
      {waterTrail.arcs.length > 0 && (
        <Source
          id="water-trail-arcs"
          type="geojson"
          data={{
            type: "FeatureCollection",
            features: waterTrail.arcs.map((arc) => ({
              type: "Feature" as const,
              properties: {
                id: arc.id,
                name: arc.display_name,
                from_node: arc.from_node,
                to_node: arc.to_node,
              },
              geometry: arc.geometry,
            })),
          } as GeoJSON.FeatureCollection}
        >
          {/* White outline */}
          <Layer
            id="water-trail-outline"
            type="line"
            paint={{
              "line-color": "#ffffff",
              "line-width": 10,
              "line-opacity": 0.9,
            }}
            layout={{ "line-cap": "round", "line-join": "round" }}
          />
          {/* Main trail - bright water blue */}
          <Layer
            id="water-trail-main"
            type="line"
            paint={{
              "line-color": "#0091ea", // Bright water blue
              "line-width": 6,
              "line-opacity": 0.9,
            }}
            layout={{ "line-cap": "round", "line-join": "round" }}
          />
          {/* Flow animation */}
          <Layer
            id="water-trail-flow"
            type="line"
            paint={{
              "line-color": "#ffffff",
              "line-width": 2,
              "line-opacity": 0.7,
              "line-dasharray": [1, 3],
            }}
          />
        </Source>
      )}

      {/* MAJOR RESERVOIRS - Always visible landmarks */}
      {majorReservoirs.map((reservoir) => {
        const isSelected = selectedReservoir?.id === reservoir.id
        
        return (
          <Marker
            key={`reservoir-${reservoir.id}`}
            longitude={reservoir.coordinates[0]}
            latitude={reservoir.coordinates[1]}
          >
            <Box
              onMouseEnter={() => setHoveredNode(reservoir)}
              onMouseLeave={() => setHoveredNode(null)}
              onClick={() => loadWaterTrail(reservoir)}
              sx={{ cursor: "pointer", position: "relative" }}
            >
              <LocationOnIcon
                sx={{
                  fontSize: isSelected ? '4rem' : '3rem',
                  color: isSelected ? '#ff6b35' : '#2563eb',
                  filter: isSelected 
                    ? 'drop-shadow(0 0 0 4px #ff6b35) drop-shadow(0 6px 20px rgba(255,107,53,0.4))'
                    : 'drop-shadow(0 4px 12px rgba(37,99,235,0.4))',
                  '&:hover': { 
                    transform: 'scale(1.1)',
                    filter: 'drop-shadow(0 6px 20px rgba(0,0,0,0.6))'
                  },
                  transition: 'all 0.3s ease',
                  zIndex: isSelected ? 10000 : 9999,
                }}
              />
              {/* Rank badge */}
              <Box
                sx={{
                  position: 'absolute',
                  top: '-12px',
                  right: '-12px',
                  backgroundColor: isSelected ? '#ff6b35' : '#2563eb',
                  color: 'white',
                  borderRadius: '50%',
                  width: '24px',
                  height: '24px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: '0.8rem',
                  fontWeight: 'bold',
                  border: '2px solid white',
                  boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
                }}
              >
                {reservoir.rank}
              </Box>
            </Box>
          </Marker>
        )
      })}

      {/* TRAIL NODES - Curated infrastructure only */}
      {waterTrail.nodes.map((node) => {
        // Skip the selected reservoir (already shown above)
        if (node.short_code === selectedReservoir?.short_code) {
          return null
        }
        
        const getInfrastructureColor = (elementType: string) => {
          switch (elementType) {
            case "STR": return "#2563eb"  // Blue reservoirs
            case "PS": return "#dc2626"   // Red pump stations  
            case "WTP": return "#059669"  // Green treatment
            case "WWTP": return "#7c3aed" // Purple wastewater
            case "CH": return "#0891b2"   // Cyan river junctions
            default: return "#6b7280"     // Gray others
          }
        }
        
        const getInfrastructureSize = (elementType: string) => {
          switch (elementType) {
            case "STR": return 24   // Large reservoirs
            case "PS": return 20    // Medium pump stations
            case "WTP": return 18   // Medium treatment
            case "WWTP": return 18  // Medium wastewater
            case "CH": return 14    // Smaller junctions
            default: return 12      // Small others
          }
        }
        
        return (
          <Marker
            key={`trail-node-${node.id}`}
            longitude={node.coordinates[0]}
            latitude={node.coordinates[1]}
          >
            <Box
              onMouseEnter={() => setHoveredNode(node)}
              onMouseLeave={() => setHoveredNode(null)}
              onClick={() => loadWaterTrail(node)}
              sx={{
                width: getInfrastructureSize(node.element_type),
                height: getInfrastructureSize(node.element_type),
                borderRadius: "50%",
                backgroundColor: getInfrastructureColor(node.element_type),
                border: "3px solid white",
                cursor: "pointer",
                transition: "all 0.3s ease",
                boxShadow: "0 3px 12px rgba(0,0,0,0.4)",
                "&:hover": {
                  transform: "scale(1.2)",
                  boxShadow: "0 6px 20px rgba(0,0,0,0.6)",
                },
                // Pulse for key infrastructure
                ...(["STR", "PS", "WTP", "WWTP"].includes(node.element_type) && {
                  animation: "infrastructurePulse 3s infinite",
                  "@keyframes infrastructurePulse": {
                    "0%, 100%": { opacity: 1 },
                    "50%": { opacity: 0.8 },
                  },
                }),
              }}
            />
          </Marker>
        )
      })}

      {/* TRAIL TOOLTIP - Enhanced with infrastructure context */}
      {hoveredNode && (
        <Popup
          longitude={hoveredNode.coordinates[0]}
          latitude={hoveredNode.coordinates[1]}
          closeButton={false}
          closeOnClick={false}
          maxWidth="400px"
        >
          <Box sx={{ padding: 1.5, minWidth: 250, maxWidth: 400 }}>
            <Typography variant="h6" sx={{ mb: 0.5, fontSize: "1.1rem", fontWeight: "bold" }}>
              {hoveredNode.display_name}
            </Typography>
            
            <Typography variant="body2" sx={{ mb: 0.5, fontSize: "0.85rem", color: "text.secondary" }}>
              {hoveredNode.short_code} • {hoveredNode.infrastructure_type || hoveredNode.element_type}
            </Typography>
            
            {/* System info for reservoirs */}
            {(hoveredNode as any).system_name && (
              <Typography variant="body2" sx={{ mb: 0.5, fontSize: "1rem", fontWeight: "bold", color: "primary.main" }}>
                <strong>System:</strong> {(hoveredNode as any).system_name}
                {hoveredNode.rank && ` • Major CA Reservoir #${hoveredNode.rank}`}
              </Typography>
            )}
            
            {/* Capacity for reservoirs */}
            {hoveredNode.capacity_taf && (
              <Typography variant="body2" sx={{ mb: 0.5, fontSize: "0.9rem", color: "info.main" }}>
                <strong>Capacity:</strong> {hoveredNode.capacity_taf.toLocaleString()} TAF
              </Typography>
            )}
            
            {/* Trail context */}
            {selectedReservoir && hoveredNode.short_code !== selectedReservoir.short_code && (
              <Typography variant="body2" sx={{ mb: 0.5, fontSize: "0.85rem", color: "info.main" }}>
                <strong>Water Trail:</strong> Connected to {selectedReservoir.display_name}
              </Typography>
            )}
            
            {/* Trail metadata */}
            {selectedReservoir?.short_code === hoveredNode.short_code && trailMetadata && (
              <Typography variant="body2" sx={{ fontSize: "0.8rem", color: "success.main", fontStyle: "italic" }}>
                <strong>Trail:</strong> {trailMetadata.trail_nodes} key facilities • {trailMetadata.trail_arcs} pathways
                <br />
                <strong>Clarity:</strong> {trailMetadata.reduction_ratio} of full network shown
              </Typography>
            )}
            
            {/* Action prompt */}
            <Typography
              variant="body2"
              sx={{
                fontSize: "0.75rem",
                fontStyle: "italic",
                mt: 1,
                color: "text.secondary",
                borderTop: "1px solid #e0e0e0",
                pt: 0.5,
              }}
            >
              {isLoadingTrail && selectedReservoir?.short_code === hoveredNode.short_code
                ? "🌊 Loading water trail (connect the dots mode)..."
                : selectedReservoir?.short_code === hoveredNode.short_code
                  ? "🎯 Showing curated water trail - click other reservoirs to explore"
                  : hoveredNode.element_type === 'STR'
                    ? "🌊 Click to trace water trail (infrastructure pathway)"
                    : "🏗️ Key water infrastructure facility"}
            </Typography>
          </Box>
        </Popup>
      )}
    </>
  )
}
