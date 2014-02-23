###
  A LOT of this code comes from the angular.js project

  additionally this uses zest for dom selection and hammer.js (with an angular-hammer wrapper) for touch and mouse-drag event support
###

'use strict'

#angular logic to go here
module = angular.module 'dragger', ['hmTouchEvents']

#Curtesy of http://css-tricks.com/snippets/javascript/move-cursor-to-end-of-input/
cuMoveCursorToEnd = (el) ->
	if not el.selectionStart? or not el.createtextRange? then el.focus()
	else
		if typeof el.selectionStart == "number" then el.selectionStart = el.selectionEnd = el.value.length
		else if typeof el.createTextRange != "undefined"
			el.focus()
			range = el.createTextRange()
			range.collapse false
			range.select()

#sets up the events directly before repeatReorder is executed, as when it executes we no longer have access to the original html until repeats start populating which is bad performance
#all hm events will correctly bubble if you'd like to do something cool like show a delete button with that functionality. Exception is hm-dragup, hm-dragdown and hm-drag when direction is up or down.
module.directive 'cuReorderHandle', ->
	transclude: false
	priority: 999
	terminal: false
	compile: (element, attr, linker) ->
		#setup the element to have correct attributes on the drag 'handle'
		dragElement = angular.element zest(attr.cuReorderHandle, element[0])[0]
		if dragElement?
			#touch compatible events - these do nothing if you don't have angular-hammer installed, so you should. For compatability see here: https://github.com/EightMedia/hammer.js/wiki/Compatibility
			dragElement.attr "hm-drag", "reorderFuncs.moveevent($event, $elementRef, $index)"
			dragElement.attr "hm-dragstart", "reorderFuncs.startevent($event, $elementRef, $index)"
			dragElement.attr "hm-dragend", "reorderFuncs.stopevent($event, $elementRef, $index)"

#this is a modification of the ngRepeat - I've commented where things have been added to keep track of the drag/drop functions
module.directive 'cuRepeatReorder', ->
	transclude: "element"
	priority: 1000
	terminal: true
	compile: (element, attr, linker) ->
		(scope, iterStartElement, attr) ->
			expression = attr.cuRepeatReorder
			match = expression.match(/^\s*(.+)\s+in\s+(.*)\s*$/)
			lhs = undefined
			rhs = undefined
			valueIdent = undefined
			keyIdent = undefined
			throw Error("Expected ngRepeat in form of '_item_ in _collection_' but got '" + expression + "'.")	unless match
			lhs = match[1]
			rhs = match[2]
			match = lhs.match(/^(?:([\$\w]+)|\(([\$\w]+)\s*,\s*([\$\w]+)\))$/)
			throw Error("'item' in 'item in collection' should be identifier or (key, value) but got '" + lhs + "'.")	 unless match
			valueIdent = match[3] or match[1]
			keyIdent = match[2]
      #here are all the data we need for reordering via drag and drop
			reorderFuncs =
				offset: 0
				gesture: 'vertical' #this is used so we only detect vertical drags, this allows for swipe left to show a delete button for example
        #a shortcut to set the top and bottom margins, also removes the top border style - in styles only
				setMargins: ($element, top="", bottom="") ->
					$element.css "margin-top", top
					$element.css "margin-bottom", bottom
					$element.css "border-top", ""
				#reset all margins to default - ie no margins/set in css
				resetMargins: -> @setMargins lastOrder.peek(c).element for c in scope.$eval(rhs)
				#shortcut to switche the dragging class on and off
				updateElementClass: ($element) ->
					if @gesture is "vertical" then $element.addClass 'dragging'
					else $element.removeClass 'dragging'
				#this function gets the offset of the mouse and manipulates the margins to reposition everything correctly
				updateOffset: ($event, $element, $index) ->
					@offset = 0
					collection = scope.$eval(rhs)
					workingDelta = $event.gesture.deltaY
					gDirection = if $event.gesture.deltaY < 0 then "up" else "down"
					directedHeight = $element[0].offsetHeight * if gDirection is "up" then -1 else 1
					workingElement = $element[0]
					halfHeight = 0

					workingDelta += directedHeight/2 # This means that the "gap" we will insert into will move when we move past the half way mark of the next element (called lenience calculation)
					while (gDirection is "down" and workingDelta > 0 and $index+@offset < collection.length) or (gDirection is "up" and workingDelta < 0 and $index+@offset >= 0) #figure on how many spaces we've moved
						if gDirection is "down" then @offset++ else @offset--
						if gDirection is "down" and $index+@offset >= collection.length
							workingElement = lastOrder.peek(collection[$index+@offset-1]).element
							break
						if gDirection is "up" and $index+@offset < 0
							workingElement = lastOrder.peek(collection[0]).element
							break
						#get the currently focussed element, then reset the margins on it to 0
						workingElement = lastOrder.peek(collection[$index+@offset]).element
						@setMargins workingElement
						#reset the one on the other side of the original position, if any. This catches if you move really fast across the list
						if collection.length > $index-@offset >= 0 and @offset isnt 0 then @setMargins lastOrder.peek(collection[$index-@offset]).element
						workingDelta += workingElement[0].offsetHeight * if gDirection is "down" then -1 else 1
					#now we have the previous/next element, we insert the correct amount of margin to show the "gap"
					if not (-1 <= @offset <= 1)
						bottomMargin = "#{workingElement.css("margin-bottom").replace(/^[0-9\.]/g, '') + $element[0].offsetHeight}px"
						topMargin = "#{workingElement.css("margin-top").replace(/^[0-9\.]/g, '') + $element[0].offsetHeight}px"
						if gDirection is "up"
							if $index+@offset < 0 then @setMargins workingElement, topMargin
							else @setMargins workingElement, "", bottomMargin
						if gDirection is "down"
							if $index+@offset >= collection.length then @setMargins workingElement, "", bottomMargin
							else @setMargins workingElement, topMargin
					#clear the remaining elements that haven't been reset in the first where
					count = 1 + if $index+@offset < 0 or $index+@offset >= collection.length then 2 else 0 # need to set 2 if at one of the end elements else this code just resets the margins again
					while $index+@offset+count < collection.length or $index+@offset-count >= 0
						if $index+@offset+count < collection.length then @setMargins lastOrder.peek(collection[$index+@offset+count]).element
						if $index+@offset-count >= 0 then @setMargins lastOrder.peek(collection[$index+@offset-count]).element
						count++
					workingDelta -= directedHeight/2 # undo the lenience calculation

					#fix the delta so that it cannot move past the first/last slots!
					if (workingDelta <= 0 and gDirection is "down") or (workingDelta >= 0 and gDirection is "up") then delta = $event.gesture.deltaY
					else delta = $event.gesture.deltaY - workingDelta
					if -1 <= @offset <= 1 #close to home so special logic to show the "gap" where we originally are
						@setMargins $element, "#{delta}px", "#{-delta}px" #this means we are hovering over our original position
						beforeIndex = $index-1
						afterIndex = $index+1
					else if @offset < 0 #going up, so show new gap
						if $index > 1 then @setMargins lastOrder.peek(collection[$index-1]).element
						@setMargins $element, "#{delta-$element[0].offsetHeight}px", "#{-(delta+(0.5*$element[0].offsetHeight))}px"
						beforeIndex = $index+@offset
						afterIndex = $index+@offset+1
					else #going down, so show new gap
						if $index < collection.length-2 then @setMargins lastOrder.peek(collection[$index+1]).element
						@setMargins $element, "#{delta-(0*$element[0].offsetHeight)}px", "#{-(delta+$element[0].offsetHeight)}px"
						beforeIndex = $index+@offset-1
						afterIndex = $index+@offset
					# re-add the dragging-before and after classes, the two elements that get these classes border the "gap" we are targeting into
					angular.element(zest(".dragging-before")).removeClass "dragging-before"
					angular.element(zest(".dragging-after")).removeClass "dragging-after"
					if beforeIndex >= 0 then lastOrder.peek(collection[beforeIndex]).element.addClass "dragging-before"
					if afterIndex < collection.length then lastOrder.peek(collection[afterIndex]).element.addClass "dragging-after"
				#to catch a move event
				moveevent: ($event, $element, $index) ->
					@updateElementClass $element
					if @gesture is "vertical"
						@updateOffset $event, $element, $index
						$event.preventDefault()
						$event.stopPropagation()
						$event.gesture.stopPropagation()
						return false
					else
						@resetMargins()
				#used for the start event
				startevent: ($event, $element, $index) ->
					$element.parent().addClass "active-drag-below"
					#we get the gesture ONCE then continue using it forever till the end
					@gesture = if $event.gesture.direction is "up" or $event.gesture.direction is "down" then "vertical" else "horizontal"
					@updateElementClass $element
					@offset = 0
					@updateOffset $event, $element
					$event.preventDefault()
				#when a drag event finishes
				stopevent: ($event, $element, $index) ->
					$element.parent().removeClass "active-drag-below"
					@resetMargins()
					angular.element(zest(".dragging-before")).removeClass "dragging-before"
					angular.element(zest(".dragging-after")).removeClass "dragging-after"
					#after animation, so before the watch is fired!
					if @offset isnt 0
						collection = scope.$eval(rhs)
						obj = collection.splice $index, 1
						if @offset < 0 then collection.splice $index + @offset + 1, 0, obj[0]
						else if @offset > 0 then collection.splice $index + @offset - 1, 0, obj[0]
					#so it shouldn't dissapear during transition
					$element.removeClass 'dragging'
					$event.preventDefault()

			# Store a list of elements from previous run. This is a hash where key is the item from the
			# iterator, and the value is an array of objects with following properties.
			#		- scope: bound scope
			#		- element: previous element.
			#		- index: position
			# We need an array of these objects since the same object can be returned from the iterator.
			# We expect this to be a rare case.
			lastOrder = new HashQueueMap()
			scope.$watch ngRepeatWatch = (scope) ->
				index = undefined
				length = undefined
				collection = scope.$eval(rhs)
				cursor = iterStartElement # current position of the node
				# Same as lastOrder but it has the current state. It will become the
				# lastOrder on the next iteration.
				nextOrder = new HashQueueMap()
				arrayBound = undefined
				childScope = undefined
				key = undefined
				value = undefined
				array = undefined
				last = undefined
				# key/value of iteration
				# last object information {scope, element, index}
				unless isArray(collection)

					# if object, extract keys, sort them and use to determine order of iteration over obj props
					array = []
					for key of collection
						array.push key	if collection.hasOwnProperty(key) and key.charAt(0) isnt "$"
					array.sort()
				else
					array = collection or []
				arrayBound = array.length - 1

				# we are not using forEach for perf reasons (trying to avoid #call)
				index = 0
				length = array.length

				while index < length
					key = (if (collection is array) then index else array[index])
					value = collection[key]
					last = lastOrder.shift(value)
					if last

						# if we have already seen this object, then we need to reuse the
						# associated scope/element
						childScope = last.scope
						nextOrder.push value, last
						if index is last.index

							# do nothing
							cursor = last.element
						else

							# existing item which got moved
							last.index = index

							# This may be a noop, if the element is next, but I don't know of a good way to
							# figure this out,	since it would require extra DOM access, so let's just hope that
							# the browsers realizes that it is noop, and treats it as such.
							cursor.after last.element
							cursor = last.element
					else

						# new item which we don't know about
						childScope = scope.$new()
					childScope[valueIdent] = value
					childScope[keyIdent] = key	if keyIdent
					childScope.$index = index
					childScope.$first = (index is 0)
					childScope.$last = (index is arrayBound)
					childScope.$middle = not (childScope.$first or childScope.$last)
          #HERE: add in the reorder funcs to the scope
					childScope.reorderFuncs = reorderFuncs
					unless last
						linker childScope, (clone) ->#clone is a copy of the original element, thanks to transclude
							cursor.after clone #this puts the repeated element on the page
							last =
								scope: childScope
								element: (cursor = clone)
								index: index
							childScope.$elementRef = last.element
							nextOrder.push value, last

					index++

				#shrink children
				for key of lastOrder
					if lastOrder.hasOwnProperty(key)
						array = lastOrder[key]
						while array.length
							value = array.pop()
							value.element.remove() #this removes the old element off the page
							value.scope.$destroy()
				lastOrder = nextOrder
				return #need this otherwise it errors

#the following are the functions angular uses to work around the ngRepeat

###
 * A map where multiple values can be added to the same key such that they form a queue.
 * @returns {HashQueueMap}
###
HashQueueMap = ->
HashQueueMap:: =

	###
	Same as array push, but using an array as the value for the hash
	###
	push: (key, value) ->
		array = this[key = hashKey(key)]
		unless array
			this[key] = [value]
		else
			array.push value


	###
	Same as array shift, but using an array as the value for the hash
	###
	shift: (key) ->
		array = this[key = hashKey(key)]
		if array
			if array.length is 1
				delete this[key]

				array[0]
			else
				array.shift()


	###
	return the first item without deleting it
	###
	peek: (key) ->
		array = this[hashKey(key)]
		array[0]	if array

isArray = (value) -> toString.apply(value) is '[object Array]'
hashKey = (obj) ->
	objType = typeof obj
	key = undefined
	if objType is "object" and obj isnt null
		if typeof (key = obj.$$hashKey) is "function"

			# must invoke on object to keep the right this
			key = obj.$$hashKey()
		else key = obj.$$hashKey = nextUid()	if key is `undefined`
	else
		key = obj
	objType + ":" + key

uid = ["0", "0", "0"]

nextUid = ->
	index = uid.length
	digit = undefined
	while index
		index--
		digit = uid[index].charCodeAt(0)
		if digit is 57 #'9'
			uid[index] = "A"
			return uid.join("")
		if digit is 90 #'Z'
			uid[index] = "0"
		else
			uid[index] = String.fromCharCode(digit + 1)
			return uid.join("")
	uid.unshift "0"
	uid.join ""