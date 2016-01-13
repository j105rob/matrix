(ns shmoocon
  (:use     [streamparse.specs])
  (:gen-class))

(defn shmoocon [options]
   [
    ;; spout configuration
    {"udp-spout" (python-spout-spec
          options
          "spouts.udpspout.UdpSpout"
          {
          	;;streams to output
          	"orig" ["orig"]
          }
          :conf {
                 "shmoocon.spouts.udpspout.emitStream","orig"
                 }
          )
	}
    ;; bolt configuration
    {
        "delay-bolt" (python-bolt-spec
	     options
	     {["udp-spout" "orig"]["orig"]}
	     "bolts.delay.Delay"
	     {
	     "orig" ["orig"]
	     }
	     :p 1
	     :conf {
	            "shmoocon.bolts.delay.emitStream","orig"
	            "topology.tick.tuple.freq.secs",0.5
	            }
	     ),
    "anchor-bolt" (python-bolt-spec
	     options
	     {["delay-bolt" "orig"]["orig"]}
	     "bolts.anchor.Anchor"
	     {
	     "orig" ["orig"]
	     }
	     :p 1
	     :conf {
	            "shmoocon.bolts.anchor.emitStream","orig"
	            "shmoocon.bolts.anchor.id","uuid"
	            }
	     ),
     "iptodot-bolt" (python-bolt-spec
          options
          {["anchor-bolt" "orig"]["orig"]}
          "bolts.iptodot.IpToDot"
          {
          "orig" ["orig"]
          }
          :p 1
          :conf {"shmoocon.iptodot.fields","{'transform':{'src_addr':'src_dot','dst_addr':'dst_dot'}}"
          		"shmoocon.iptodot.emitStream","orig"
          		}
          ),
     "hilbert-bolt" (python-bolt-spec
          options
          {["iptodot-bolt" "orig"]["orig"]}
          "bolts.hilbert.HilbertBolt"
          {
          "orig" ["orig"]
          }
          :p 1
          :conf {
          		"shmoocon.hilbert.fields","{'id':'uuid','src':'src_addr','dst':'dst_addr'}"
          		"shmoocon.hilbert.emitStream","orig"
          		}
          ),
    "img-bolt" (python-bolt-spec
	     options
	     {["hilbert-bolt" "orig"]["orig"]}
	     "bolts.pngmaker.PngMaker"
	     {
	     "orig" ["orig"]
	     }
	     :p 1
	     :conf {
	            "shmoocon.bolts.pngmaker.emitStream","orig"
	            "shmoocon.bolts.pngmaker.id","uuid"
	            }
	     ),
    }
  ]
)
